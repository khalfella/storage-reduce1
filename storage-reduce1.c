#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/list.h>
#include <sys/avl.h>
#include <string.h>
#include <math.h>
#include <stddef.h>
#include <sys/mman.h>

#include <jansson.h>

#define	MAXNS			6
#define	MAXNSLEN		16
#define	MAXPATHLEN		1024
#define	UUIDLEN			64

#define	RTYPE_OBJECT		0
#define	RTYPE_DIRECTORY		1

#define	MIN_SIZE		131072

#define	MAX(a, b)		(((a) > (b)) ? (a) : (b))
#define	ROUND_UP(x, n)		((((x) + (n) - 1) / (n)) * (n))

typedef struct record_s {
	char		r_own_uuid[UUIDLEN];	/* owner uuid		*/
	char		r_obj_uuid[UUIDLEN];	/* object uuid		*/
	char		r_ns[MAXNSLEN];		/* namespace		*/
	int		r_type;			/* dir or obj		*/
	int		r_sharks_cnt;		/* number of copies	*/
	uint64_t	r_len;			/* obj size (bytes)	*/
} record_t;

typedef struct obj_s {
	char		o_uuid[UUIDLEN];	/* obj uuid		*/
	uint64_t	o_size;			/* size in bytes	*/
	int		o_nsid;			/* namespace id		*/
	avl_node_t	o_avlnode;		/* owner_t->o_objtree	*/
	list_node_t	o_lnode;		/* owner_t->o_objlist	*/
} obj_t;

typedef struct owner_s {
	char		o_uuid[UUIDLEN];	/* owner uuid		*/
	avl_tree_t	o_objtree;		/* tree of objects	*/
	list_t		o_objlist;		/* list of objects	*/
	uint64_t	o_dirs[MAXNS];		/* # of directories[ns]	*/
	uint64_t	o_objs[MAXNS];		/* # of objects[ns]	*/
	uint64_t	o_keys[MAXNS];		/* # of keys[ns]	*/
	uint64_t	o_bytes[MAXNS];		/* # of bytes[ns]	*/
	avl_node_t	o_avlnode;		/* rjob_t->rj_towners	*/
	list_node_t	o_lnode;		/* rjob_t->rj_lowners	*/
} owner_t;

/* Buffers are used to pass json text to the job theads. */
typedef struct buff_s {
	char		*b_buff;
	size_t		b_len;
	list_node_t	b_lnode;
} buff_t;

/*
 * Reduce jobs do:
 *	- Take json text from the main thread.
 *	- Parse json text into a record_t.
 *	- Create owner_t/obj_t if needed.
 *	- Update owner usage dirs, objs, keys, and bytes.
 *	- When done jobs are merged together.
 */
typedef struct rjob_s {
	int		rj_idx;		/* rjob index			*/
	pthread_t	rj_thread;	/* thread object		*/
	struct rjob_s	*rj_next;	/* next job. Used when merging	*/
	char		**rj_ns;	/* namespaces array		*/
	int		rj_nscnt;	/* number of namespaces		*/

	avl_tree_t	rj_towners;	/* Tree of owners		*/
	list_t		rj_lowners;	/* List of owners		*/

	list_t		*rj_abufslst;	/* Active buffers list		*/
	list_t		*rj_fbufslst;	/* Free buerfers list		*/
	pthread_mutex_t	*rj_abuflk;	/* mutexes and cond. variables	*/
	pthread_mutex_t	*rj_fbuflk;
	pthread_cond_t	*rj_abufcnd;
	pthread_cond_t	*rj_fbufcnd;

	int		rj_map_fd;	/* mapped file desc.		*/
	off_t		rj_map_sz;	/* mapped size in bytes		*/
	void		*rj_map_addr;	/* mapped address		*/
	off_t		rj_map_off;	/* current offset. next alloc	*/
} rjob_t;

/*
 * To compare two tree nodes. Since uuid is the first element in obj_t
 * and owner_t, we use the one function for both trees.
 */
static int
avl_comparator(const void *l, const void *r)
{
	int cmp;
	if ((cmp = strcmp((char *) l, (char *) r)) != 0)
		return (cmp/abs(cmp));
	return (0);
}

/*
 * Utility functions.
 */
static void *
safe_malloc(size_t sz)
{
	char *ret;
	if ((ret = malloc(sz)) == NULL) {
		fprintf(stderr, "%s: Failed to allocate memory\n", __func__);
		exit(2);
	}
	return (ret);
}

static void *
safe_zmalloc(size_t sz)
{
	return (memset(safe_malloc(sz), 0, sz));
}

static char *
safe_strdup(const char *s)
{
	char *ret;
	if ((ret = strdup(s)) == NULL) {
		fprintf(stderr, "%s: Failed to allocate memory\n", __func__);
		exit(2);
	}
	return (ret);
}

static int
is_power_of_two(int x)
{
	return (x && (!(x&(x-1))));
}

/*
 * Mapped memory allocation routines.
 * Note: We don't free mapped memory.
 */
static void *
rjob_malloc(rjob_t *rj, size_t sz)
{
	off_t off;
	void *ret;

	off = ROUND_UP(rj->rj_map_off + sz, 8);
	if (off >= rj->rj_map_sz) {
		fprintf(stderr, "%s: Failed to allocated from "
		    "mapped memory\n", __func__);
		exit(2);
		return (NULL);
	}

	ret = rj->rj_map_addr + rj->rj_map_off;
	rj->rj_map_off = off;
	return (ret);
}

static void *
rjob_zmalloc(rjob_t *rj, size_t sz)
{
	return (memset(rjob_malloc(rj, sz), 0, sz));
}

static int
rjob_init_mapped_memory(rjob_t *rj, const char *tempdir, off_t map_size)
{
	void *addr;
	int oflags, fd, mprot;
	char fname[MAXPATHLEN];
	struct stat sbuf;

	if (stat(tempdir, &sbuf) != 0 || !(S_IFDIR &sbuf.st_mode)) {
		fprintf(stderr, "%s: Failed to access temp directory "
		    "\"%s\" or not a directory\n", __func__, tempdir);
		return (1);
	}

	if (sprintf(fname, "%s/reducer_thread_%d", tempdir, rj->rj_idx) < 0) {
		fprintf(stderr, "%s: Failed to generate thread "
		    "mapped filename\n", __func__);
		return (1);
	}

	oflags = O_RDWR | O_CREAT | O_TRUNC;
	if ((fd = open(fname, oflags, 0660)) == -1) {
		fprintf(stderr, "%s: Failed to open thread "
		    "mapped file\n", __func__);
		return (1);
	}

	rj->rj_map_fd = fd;
	rj->rj_map_sz = map_size;
	if (pwrite(rj->rj_map_fd, "", 1, map_size - 1) != 1) {
		fprintf(stderr, "%s: Failed to create thread "
		    "sparse file\n", __func__);
		return (1);
	}

	mprot = PROT_READ | PROT_WRITE;
	if ((addr = mmap(NULL, map_size, mprot,
	    MAP_SHARED, fd, 0)) == MAP_FAILED) {
		fprintf(stderr, "%s: Failed to map thread "
		    "sparse file\n", __func__);
		return (1);
	}
	rj->rj_map_addr = addr;
	rj->rj_map_off = 0;

	return (0);
}

static int
nsidx(rjob_t *rj, char *ns)
{
	char **jns;

	jns = rj->rj_ns;
	while (*jns != NULL) {
		if (strcmp(*jns, ns) == 0)
			return (jns - rj->rj_ns);
		jns++;
	}

	return (-1);
}

static char *
nsname(rjob_t *rj, int idx)
{
	if (idx >= 0 && idx < rj->rj_nscnt)
		return (rj->rj_ns[idx]);

	fprintf(stderr, "%s: Filed to find namespace = %d\n",
	    __func__, idx);
	exit(2);
	return (NULL);
}

static buff_t *
get_buff(list_t *bufflst, pthread_mutex_t *lstlk, pthread_cond_t *lstcnd)
{
	buff_t *buff;
	pthread_mutex_lock(lstlk);
	while (list_is_empty(bufflst)) {
		pthread_cond_wait(lstcnd, lstlk);
	}
	buff = list_remove_tail(bufflst);
	pthread_mutex_unlock(lstlk);
	return (buff);
}

static void
put_buff(buff_t *buff, list_t *bufflst, pthread_mutex_t *lstlk,
    pthread_cond_t *lstcnd)
{
	pthread_mutex_lock(lstlk);
	list_insert_head(bufflst, buff);
	pthread_cond_signal(lstcnd);
	pthread_mutex_unlock(lstlk);
}


/*
 *	- A uuids are 36 character long.
 *	- Namespace starts at offset 38.
 *
 *	  1				       1
 *	->||<--------------  36  ------------>||<--
 *	  /639e843a-6519-479e-b8d8-147ebf8f5c1a/public/
 *						^
 *					38------|
 */

#define	UUID_STR_LEN	36
#define	NAMESPACE_OFF	(UUID_STR_LEN + 2)	/* /:uuid/:namespace	*/
int
json_to_record(char *json, record_t *r)
{
	const char *str, *p;
	json_t *jobj, *jprop;

	jobj = NULL;
	if ((jobj = json_loads(json, JSON_REJECT_DUPLICATES, NULL)) == NULL)
		goto error;

	if ((jprop = json_object_get(jobj, "key")) == NULL ||
	    (str = json_string_value(jprop)) == NULL ||
	    strlen(str) < (NAMESPACE_OFF + 1)) {
		goto error;
	}


	if ((p = strchr(str + NAMESPACE_OFF, '/')) == NULL) {
		if (strlen(str + NAMESPACE_OFF) > MAXNSLEN -1)
			goto error;
		strcpy(r->r_ns, str + NAMESPACE_OFF);
	} else {
		strncpy(r->r_ns, str + NAMESPACE_OFF, p - str - NAMESPACE_OFF);
		r->r_ns[p - str - NAMESPACE_OFF] = '\0';
	}


	if ((jprop = json_object_get(jobj, "type")) == NULL ||
	    (str = json_string_value(jprop)) == NULL) {
		goto error;
	}

	if (strcmp(str, "directory") == 0)
		r->r_type = RTYPE_DIRECTORY;
	else if (strcmp(str, "object") == 0)
		r->r_type = RTYPE_OBJECT;
	else
		goto error;


	r->r_sharks_cnt = 0;
	r->r_len = 0;
	r->r_obj_uuid[0] = '\0';
	if (r->r_type == RTYPE_OBJECT) {
		if ((jprop = json_object_get(jobj, "sharks")) == NULL)
			goto error;

		r->r_sharks_cnt = json_array_size(jprop);
		if ((jprop = json_object_get(jobj, "contentLength")) == NULL)
			goto error;
		r->r_len = json_integer_value(jprop);

		if ((jprop = json_object_get(jobj, "objectId")) == NULL ||
		    (str = json_string_value(jprop)) == NULL) {
			goto error;
		}
		strcpy(r->r_obj_uuid,  str);
	}

	if ((jprop = json_object_get(jobj, "owner")) == NULL ||
	    (str = json_string_value(jprop)) == NULL) {
		goto error;
	}

	strcpy(r->r_own_uuid, str);
	json_decref(jobj);
	return (0);
error:
	fprintf(stderr, "Failed to parse json object `%s`\n", json);
	json_decref(jobj);
	return (1);
}

static void
rjob_process_record(rjob_t *rj, record_t *rec)
{
	owner_t own, *pown;
	obj_t obj, *pobj;
	int nsid;

	/* skip records in other namespaces */
	if ((nsid = nsidx(rj, rec->r_ns)) == -1)
		return;

	strcpy(own.o_uuid, rec->r_own_uuid);
	if ((pown = avl_find(&rj->rj_towners, &own, NULL)) == NULL) {
		pown = rjob_zmalloc(rj, sizeof (owner_t));
		strcpy(pown->o_uuid, rec->r_own_uuid);
		avl_add(&rj->rj_towners, pown);

		avl_create(&pown->o_objtree, avl_comparator,
		    sizeof (obj_t), offsetof(obj_t, o_avlnode));
		list_create(&pown->o_objlist, sizeof (obj_t),
		    offsetof(obj_t, o_lnode));
	}

	nsid = nsidx(rj, rec->r_ns);

	if (rec->r_type == RTYPE_DIRECTORY) {
		pown->o_dirs[nsid]++;
		return;
	}

	strcpy(obj.o_uuid, rec->r_obj_uuid);
	if ((pobj = avl_find(&pown->o_objtree, &obj, NULL)) == NULL) {
		pobj = rjob_zmalloc(rj, sizeof (obj_t));
		strcpy(pobj->o_uuid, rec->r_obj_uuid);
		pobj->o_nsid = nsid;
		pobj->o_size = MAX(rec->r_len, MIN_SIZE) * rec->r_sharks_cnt;
		avl_add(&pown->o_objtree, pobj);

		pown->o_objs[nsid]++;
		pown->o_bytes[nsid] += pobj->o_size;
	}

	pown->o_keys[nsid]++;
}

static void
rjob_avltree_to_list(rjob_t *rj)
{
	obj_t *pobj;
	owner_t *pown;

	for (pown = avl_first(&rj->rj_towners); pown != NULL;
	    pown = AVL_NEXT(&rj->rj_towners, pown)) {

		list_insert_tail(&rj->rj_lowners, pown);
		for (pobj = avl_first(&pown->o_objtree); pobj != NULL;
		    pobj = AVL_NEXT(&pown->o_objtree, pobj)) {
			list_insert_tail(&pown->o_objlist, pobj);
		}
	}
}

static void *
rjob_scan(void *arg)
{
	rjob_t *rj;
	buff_t *buff;
	record_t rec;

	rj = (rjob_t *) arg;

	while (1) {
		buff = get_buff(rj->rj_abufslst, rj->rj_abuflk, rj->rj_abufcnd);
		if (buff->b_buff == NULL) {
			put_buff(buff, rj->rj_fbufslst,
			    rj->rj_fbuflk, rj->rj_fbufcnd);
			break;
		}
		if (json_to_record(buff->b_buff, &rec) != 0) {
			fprintf(stderr, "Invalid input record \"%s\"\n",
			    buff->b_buff);
			exit(2);
		}

		put_buff(buff, rj->rj_fbufslst, rj->rj_fbuflk, rj->rj_fbufcnd);
		rjob_process_record(rj, &rec);
	}

	rjob_avltree_to_list(rj);
	return (NULL);
}

static void
rjob_merge_owners(owner_t *own1, owner_t *own2)
{
	int i, cmp, nsid;
	list_t olist, *list1, *list2;
	obj_t *obj, *obj1, *obj2;

	list1 = &own1->o_objlist;
	list2 = &own2->o_objlist;
	obj1 = list_head(list1);
	obj2 = list_head(list2);
	list_create(&olist, sizeof (obj_t), offsetof(obj_t, o_lnode));

	while (obj1 != NULL || obj2 != NULL) {
		if (obj1 != NULL && obj2 != NULL) {
			if ((cmp = strcmp(obj1->o_uuid, obj2->o_uuid)) == 0) {

				nsid = obj2->o_nsid;
				own2->o_keys[nsid]--;
				own2->o_objs[nsid]--;
				own2->o_bytes[nsid] -= obj2->o_size;

				obj = list_next(list2, obj2);
				list_remove(list2, obj2);
				/*
				 * This object was allocated
				 * from the mapped No need to
				 * free it.
				 */
				obj2 = obj;

				own1->o_keys[nsid]++;

				obj = list_next(list1, obj1);
				list_remove(list1, obj1);
				list_insert_tail(&olist, obj1);
				obj1 = obj;

			} else if (cmp < 0) {
				obj = list_next(list1, obj1);
				list_remove(list1, obj1);
				list_insert_tail(&olist, obj1);
				obj1 = obj;
			} else { /* (cmp > 0) */

				nsid = obj2->o_nsid;

				own2->o_keys[nsid]--;
				own2->o_objs[nsid]--;
				own2->o_bytes[nsid] -= obj2->o_size;

				own1->o_objs[nsid]++;
				own1->o_keys[nsid]++;
				own1->o_bytes[nsid] += obj2->o_size;

				obj = list_next(list2, obj2);
				list_remove(list2, obj2);
				list_insert_tail(&olist, obj2);
				obj2 = obj;
			}
		} else {
			if (obj1 == NULL && obj2 != NULL) {
				list_move_tail(&olist, list2);
			} else { /* (obj1 != NULL && obj2 == NULL) */
				list_move_tail(&olist, list1);
			}
			break;
		}
	}


	if (!list_is_empty(list1) || !list_is_empty(list2)) {
		fprintf(stderr, "%s: Error one of the lists is not empty!\n",
		    __func__);
		exit(1);
	}

	for (i = 0; i < MAXNS; i++) {
		own1->o_dirs[i] += own2->o_dirs[i];
		own1->o_objs[i] += own2->o_objs[i];
		own1->o_keys[i] += own2->o_keys[i];
		own1->o_bytes[i] += own2->o_bytes[i];
		own2->o_dirs[i] = 0;
		own2->o_objs[i] = 0;
		own2->o_keys[i] = 0;
		own2->o_bytes[i] = 0;
	}


	/* Put the final result in own1 */
	list_move_tail(&own1->o_objlist, &olist);
}

static void *
rjob_merge(void *arg)
{
	int cmp;
	rjob_t *rj1, *rj2;
	owner_t *own, *own1, *own2;
	list_t olist, *list1, *list2;

	rj1 = (rjob_t *) arg;
	rj2 = rj1->rj_next;
	list1 = &rj1->rj_lowners;
	list2 = &rj2->rj_lowners;
	own1 = list_head(list1);
	own2 = list_head(list2);

	list_create(&olist, sizeof (owner_t), offsetof(owner_t, o_lnode));

	while (own1 != NULL || own2 != NULL) {
		if (own1 != NULL && own2 != NULL) {

			if ((cmp = strcmp(own1->o_uuid, own2->o_uuid)) == 0) {

				rjob_merge_owners(own1, own2);

				own = list_next(list2, own2);
				list_remove(list2, own2);
				/*
				 * No need to free own2, it
				 * was allocated from mapped
				 * memory
				 */
				own2 = own;

				own = list_next(list1, own1);
				list_remove(list1, own1);
				list_insert_tail(&olist, own1);
				own1 = own;

			} else if (cmp < 0) {
				own = list_next(list1, own1);
				list_remove(list1, own1);
				list_insert_tail(&olist, own1);
				own1 = own;
			} else { /* (cmp > 0) */
				own = list_next(list2, own2);
				list_remove(list2, own2);
				list_insert_tail(&olist, own2);
				own2 = own;
			}

		} else {

			if (own1 == NULL && own2 != NULL) {
				list_move_tail(&olist, list2);
			} else { /* (own1 != NULL && own2 == NULL) */
				list_move_tail(&olist, list1);
			}
			break;
		}
	}

	if (!list_is_empty(list1) || !list_is_empty(list2)) {
		fprintf(stderr, "%s: Error one of the lists is not empty\n",
		    __func__);
		exit(1);
	}

	/* Put the ending result in job1's list */
	list_move_tail(&rj1->rj_lowners, &olist);
	return (NULL);
}

static void
scan_input_stdin(rjob_t *rjobs, int nthreads)
{
	int i;
	ssize_t len;
	void *status;
	buff_t *buff, *buffs;
	list_t abuflst, fbuflst;
	pthread_mutex_t abuflk;
	pthread_mutex_t fbuflk;
	pthread_cond_t	abufcnd;
	pthread_cond_t	fbufcnd;

	pthread_mutex_init(&abuflk, NULL);
	pthread_mutex_init(&fbuflk, NULL);
	pthread_cond_init(&abufcnd, NULL);
	pthread_cond_init(&fbufcnd, NULL);
	list_create(&abuflst, sizeof (buff_t), offsetof(buff_t, b_lnode));
	list_create(&fbuflst, sizeof (buff_t), offsetof(buff_t, b_lnode));

	buffs = safe_zmalloc(sizeof (buff_t) * nthreads * 2);
	for (i = 0; i < 2 * nthreads; i ++)
		list_insert_tail(&fbuflst, &buffs[i]);

	/* Start rjob threads */
	for (i = 0; i < nthreads; i++) {
		rjobs[i].rj_abufslst = &abuflst;
		rjobs[i].rj_fbufslst = &fbuflst;
		rjobs[i].rj_abuflk = &abuflk;
		rjobs[i].rj_fbuflk = &fbuflk;
		rjobs[i].rj_abufcnd = &abufcnd;
		rjobs[i].rj_fbufcnd = &fbufcnd;

		if (pthread_create(&rjobs[i].rj_thread, NULL,
		    rjob_scan, &rjobs[i]) != 0) {
			fprintf(stderr, "Failed to create scanner thread\n");
			exit(1);
		}
	}

	/* Read lines from stdin and put the buffers in the active list */
	buff = get_buff(&fbuflst, &fbuflk, &fbufcnd);
	while ((len = getline(&buff->b_buff, &buff->b_len, stdin)) != -1) {
		if (len > 0) {
			if (buff->b_buff[len - 1] == '\n') {
				buff->b_buff[len - 1] = 0;
			}
			put_buff(buff, &abuflst, &abuflk, &abufcnd);
			buff = get_buff(&fbuflst, &fbuflk, &fbufcnd);
			continue;
		}

		/* empty input */
		break;
	}
	put_buff(buff, &fbuflst, &fbuflk, &fbufcnd);

	/* An empty buffer signals rjob threads the end of input */
	for (i = 0; i < nthreads; i++) {
		buff = get_buff(&fbuflst, &fbuflk, &fbufcnd);
		free(buff->b_buff);
		buff->b_buff = NULL;
		buff->b_len = 0;
		put_buff(buff, &abuflst, &abuflk, &abufcnd);
	}

	/* Wait for all reducers to finish */
	for (i = 0; i < nthreads; i++) {
		pthread_join(rjobs[i].rj_thread, &status);
	}

	/* Merge */
	while (nthreads != 1) {
		for (i = 0; i < nthreads / 2; i++) {
			rjobs[i].rj_next = &rjobs[i + nthreads / 2];
			if (pthread_create(&rjobs[i].rj_thread, NULL,
			    rjob_merge, &rjobs[i]) != 0) {
				fprintf(stderr, "Failed to create merger "
				    "thread\n");
				exit(1);
			}
		}

		for (i = 0; i < nthreads / 2; i++) {
			pthread_join(rjobs[i].rj_thread, &status);
		}
		nthreads /= 2;
	}
}

static void
output(rjob_t *rj)
{
	int n;
	owner_t *own;
	for (own = list_head(&rj->rj_lowners); own != NULL;
	    own = list_next(&rj->rj_lowners, own)) {
		for (n = 0; n < rj->rj_nscnt; n++) {
			fprintf(stdout, "{"
			    "\"owner\":\"%s\","
			    "\"namespace\":\"%s\","
			    "\"directories\":%" PRIu64 ","
			    "\"keys\":%" PRIu64 ","
			    "\"objects\":%" PRIu64 ","
			    "\"bytes\":\"%" PRIu64 "\"}\n",
			    own->o_uuid, nsname(rj, n), own->o_dirs[n],
			    own->o_keys[n], own->o_objs[n],
			    own->o_bytes[n]);
		}
	}
}

#define	NAMESPACES	"stor public jobs reports"

static char **
parse_namespaces(char *ens, int *cnt)
{
	char *n, *sns, *ns, **nspaces;
	if (ens == NULL && (ens = getenv("NAMESPACES")) == NULL)
		ens = NAMESPACES;

	*cnt = 0;
	sns = ns = safe_strdup(ens);
	nspaces = safe_malloc(sizeof (char *) * strlen(ens) + 1);
	while ((n = strsep(&ns, " ")) != NULL) {
		if (*n == '\0')
			continue;
		nspaces[(*cnt)++] = safe_strdup(n);
	}
	free(sns);
	nspaces[*cnt] = NULL;

	if (*cnt <= MAXNS)
		return (nspaces);

	while (*nspaces != NULL)
		free(*nspaces++);

	return (NULL);
}

#define	MB2B(x)		((x) * 1048576UL)

#define	NTHREADS	16		/* By default start 8 threads	*/
#define	TMEMSIZE	128		/* 128 MB of memory per thread	*/
#define	TEMPDIR		"/var/tmp"	/* Use /var/tmp by default	*/

static void
usage(int ecode)
{
	fprintf(stderr, "Usage: storage-reduce1 "
	    "[-t nthreads] [-n namespaces] [-d tempdir]\n"
	    "\t[-m tmem] [-h]\n"
	    "\t\n"
	    "\t-t number of reducer threads (default: %d)\n"
	    "\t-n namespaces (default: \"%s\")\n"
	    "\t-d temp directory (default: \"%s\")\n"
	    "\t-m mapped memory per thread in megabytes (default: %d)\n",
	    NTHREADS, NAMESPACES, TEMPDIR, TMEMSIZE);
	exit(ecode);
}

int
main(int argc, char **argv)
{
	char c;
	char *nspcs = NULL, *tempdir = TEMPDIR;
	int i, nscnt, tmem = TMEMSIZE, nthreads = NTHREADS;
	rjob_t *rjobs;
	char **nspaces;

	while ((c = getopt(argc, argv, "t:n:d:m:h")) != EOF) {
		switch (c) {
			case 't':
				if ((nthreads = strtol(
				    optarg, NULL, 10)) <= 0 ||
				    nthreads <= 0) {
					fprintf(stderr, "Invalid number of "
					    "threads \"%s\"\n", optarg);
					exit(1);
				}
				break;
			case 'n':
				nspcs = optarg;
				break;
			case 'd':
				tempdir = optarg;
				break;
			case 'm':
				if ((tmem = strtol(
				    optarg, NULL, 10)) <= 0 ||
				    tmem <= 0) {
					fprintf(stderr, "Invalid per thread "
					    "thread memory size \"%s\"\n",
					    optarg);
					exit(1);
				}
				break;
			case 'h':
				usage(0);
				break;
			default:
				usage(1);
				break;
		}
	}

	if (!is_power_of_two(nthreads)) {
		fprintf(stderr, "nthreads is not power of two\n");
		exit(1);
	}

	if ((nspaces = parse_namespaces(nspcs, &nscnt)) == NULL) {
		fprintf(stderr, "Failed to parse namespaces\n");
		exit(1);
	}

	rjobs = safe_zmalloc(sizeof (rjob_t) * nthreads);

	for (i = 0; i < nthreads; i++) {
		rjobs[i].rj_idx = i;
		rjobs[i].rj_ns = nspaces;
		rjobs[i].rj_nscnt = nscnt;
		avl_create(&rjobs[i].rj_towners, avl_comparator,
		    sizeof (owner_t), offsetof(owner_t, o_avlnode));
		list_create(&rjobs[i].rj_lowners,
		    sizeof (owner_t), offsetof(owner_t, o_lnode));

		/* Initialize mapped memory */
		if (rjob_init_mapped_memory(&rjobs[i],
		    tempdir, MB2B(tmem)) != 0) {
			fprintf(stderr, "Failed to initialize mapped "
			    "memory idx = %d\n", i);
			exit(2);
		}
	}

	scan_input_stdin(rjobs, nthreads);
	output(rjobs);

	return (0);
}
