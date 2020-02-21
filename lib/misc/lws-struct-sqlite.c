/*
 * libwebsockets - small server side websockets and web server implementation
 *
 * Copyright (C) 2010 - 2020 Andy Green <andy@warmcat.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include <libwebsockets.h>
#include <private-lib-core.h>

#include <sqlite3.h>

/*
 * we get one of these per matching result from the query
 */

static int
lws_struct_sq3_deser_cb(void *priv, int cols, char **cv, char **cn)
{
	lws_struct_args_t *a = (lws_struct_args_t *)priv;
	const lws_struct_map_t *map = a->map_st[0];
	int n, mems = a->map_entries_st[0];
	lws_dll2_owner_t *o = (lws_dll2_owner_t *)a->cb_arg;
	char *u = lwsac_use_zero(&a->ac, a->dest_len, a->ac_block_size);
	long long li;
	size_t lim;
	char **pp;
	char *s;

	if (!u) {
		lwsl_err("OOM\n");

		return 1;
	}

	lws_dll2_add_tail((lws_dll2_t *)((char *)u + a->toplevel_dll2_ofs), o);

	while (mems--) {
		for (n = 0; n < cols; n++) {
			if (!cv[n] || strcmp(cn[n], map->colname))
				continue;

			switch (map->type) {
			case LSMT_SIGNED:
				if (map->aux == sizeof(signed char)) {
					signed char *pc;
					pc = (signed char *)(u + map->ofs);
					*pc = atoi(cv[n]);
					break;
				}
				if (map->aux == sizeof(short)) {
					short *ps;
					ps = (short *)(u + map->ofs);
					*ps = atoi(cv[n]);
					break;
				}
				if (map->aux == sizeof(int)) {
					int *pi;
					pi = (int *)(u + map->ofs);
					*pi = atoi(cv[n]);
					break;
				}
				if (map->aux == sizeof(long)) {
					long *pl;
					pl = (long *)(u + map->ofs);
					*pl = atol(cv[n]);
					break;
				}
				{
					long long *pll;
					pll = (long long *)(u + map->ofs);
					*pll = atoll(cv[n]);
				}
				break;

			case LSMT_UNSIGNED:
				if (map->aux == sizeof(unsigned char)) {
					unsigned char *pc;
					pc = (unsigned char *)(u + map->ofs);
					*pc = atoi(cv[n]);
					break;
				}
				if (map->aux == sizeof(unsigned short)) {
					unsigned short *ps;
					ps = (unsigned short *)(u + map->ofs);
					*ps = atoi(cv[n]);
					break;
				}
				if (map->aux == sizeof(unsigned int)) {
					unsigned int *pi;
					pi = (unsigned int *)(u + map->ofs);
					*pi = atoi(cv[n]);
					break;
				}
				if (map->aux == sizeof(unsigned long)) {
					unsigned long *pl;
					pl = (unsigned long *)(u + map->ofs);
					*pl = atol(cv[n]);
					break;
				}
				{
					unsigned long long *pll;
					pll = (unsigned long long *)(u + map->ofs);
					*pll = atoll(cv[n]);
				}
				break;

			case LSMT_BOOLEAN:
				li = 0;
				if (!strcmp(cv[n], "true") ||
				    !strcmp(cv[n], "TRUE") || cv[n][0] == '1')
					li = 1;
				if (map->aux == sizeof(char)) {
					char *pc;
					pc = (char *)(u + map->ofs);
					*pc = (char)li;
					break;
				}
				if (map->aux == sizeof(int)) {
					int *pi;
					pi = (int *)(u + map->ofs);
					*pi = (int)li;
				} else {
					uint64_t *p64;
					p64 = (uint64_t *)(u + map->ofs);
					*p64 = li;
				}
				break;

			case LSMT_STRING_CHAR_ARRAY:
				s = (char *)(u + map->ofs);
				lim = map->aux - 1;
				lws_strncpy(s, cv[n], lim);
				break;

			case LSMT_STRING_PTR:
				pp = (char **)(u + map->ofs);
				lim = strlen(cv[n]);
				s = lwsac_use(&a->ac, lim + 1, a->ac_block_size);
				if (!s)
					return 1;
				*pp = s;
				memcpy(s, cv[n], lim);
				s[lim] = '\0';
				break;
			default:
				break;
			}
		}
		map++;
	}

	return 0;
}

/*
 * Call this with an LSM_SCHEMA map, its colname is the table name and its
 * type information describes the toplevel type.  Schema is dereferenced and
 * put in args before the actual sq3 query, which is given the child map.
 */

int
lws_struct_sq3_deserialize(sqlite3 *pdb, const lws_struct_map_t *schema,
			   lws_dll2_owner_t *o, struct lwsac **ac,
			   uint64_t start, int limit)
{
	char s[150], where[32];
	lws_struct_args_t a;

	memset(&a, 0, sizeof(a));
	a.cb_arg = o; /* lws_dll2_owner tracking query result objects */
	a.map_st[0]  = schema->child_map;
	a.map_entries_st[0] = schema->child_map_size;
	a.dest_len = schema->aux; /* size of toplevel object to allocate */
	a.toplevel_dll2_ofs = schema->ofs;

	lws_dll2_owner_clear(o);

	where[0] = '\0';
	if (start)
		lws_snprintf(where, sizeof(where), " where when < %llu ",
				(unsigned long long)start);

	lws_snprintf(s, sizeof(s) - 1, "select * "
		     "from %s %s order by _lws_idx desc limit %d;",
		     schema->colname, where, limit);

	if (sqlite3_exec(pdb, s, lws_struct_sq3_deser_cb, &a, NULL) != SQLITE_OK) {
		lwsl_err("%s: %s: fail\n", __func__, sqlite3_errmsg(pdb));
		lwsac_free(&a.ac);
		return -1;
	}

	*ac = a.ac;

	return 0;
}

/*
 * This takes a struct and turns it into an sqlite3 UPDATE, using the given
 * schema... which has one LSM_SCHEMA_DLL2 entry wrapping the actual schema
 */

static int
_lws_struct_sq3_ser_one(sqlite3 *pdb, const lws_struct_map_t *schema,
			uint32_t idx, void *st)
{
	const lws_struct_map_t *map = schema->child_map;
	int nentries = schema->child_map_size;
	size_t sql_est = 46 + strlen(schema->colname) + 1;
		/* "insert into  (_lws_idx, ) values (00000001,);" ...
		 * plus the table name */
	uint8_t *stb = (uint8_t *)st;
	const char *p;
	char *sql;
	int n, m;

	/*
	 * Figure out an estimate for the length of the populated sqlite
	 * command, and then malloc it up
	 */

	for (n = 0; n < nentries; n++) {
		sql_est += strlen(map[n].colname) + 2;
		switch (map[n].type) {
		case LSMT_SIGNED:
		case LSMT_UNSIGNED:
		case LSMT_BOOLEAN:

			switch (map[n].aux) {
			case 1:
				sql_est += 3 + 2;
				break;
			case 2:
				sql_est += 5 + 2;
				break;
			case 4:
				sql_est += 10 + 2;
				break;
			case 8:
				sql_est += 20 + 2;
				break;
			}

			if (map[n].type == LSMT_SIGNED)
				sql_est++; /* minus sign */

			break;
		case LSMT_STRING_CHAR_ARRAY:
			sql_est += lws_sql_purify_len((const char *)st +
							map[n].ofs) + 2;
			break;

		case LSMT_STRING_PTR:
			p = *((const char * const *)&stb[map[n].ofs]);
			sql_est += lws_sql_purify_len(p) + 2;
			break;

		default:
			lwsl_err("%s: unsupported type\n", __func__);
			assert(0);
			break;
		}
	}

	sql = malloc(sql_est);
	if (!sql)
		return -1;

	m = lws_snprintf(sql, sql_est, "insert into %s(_lws_idx, ",
			 schema->colname);

	for (n = 0; n < nentries; n++)
		m += lws_snprintf(sql + m, sql_est - m,
				  n == nentries - 1 ? "%s" : "%s, ",
				  map[n].colname);

	m += lws_snprintf(sql + m, sql_est - m, ") values(%u, ", idx);

	for (n = 0; n < nentries; n++) {
		uint64_t uu64;
		size_t q;

		switch (map[n].type) {
		case LSMT_SIGNED:
		case LSMT_UNSIGNED:
		case LSMT_BOOLEAN:

			uu64 = 0;
			for (q = 0; q < map[n].aux; q++)
				uu64 |= ((uint64_t)stb[map[n].ofs + q] <<
								(q << 3));

			if (map[n].type == LSMT_SIGNED)
				m += lws_snprintf(sql + m, sql_est - m, "%lld",
						  (long long)(int64_t)uu64);
			else
				m += lws_snprintf(sql + m, sql_est - m, "%llu",
						  (unsigned long long)uu64);
			break;

		case LSMT_STRING_CHAR_ARRAY:
			sql[m++] = '\'';
			lws_sql_purify(sql + m, (const char *)&stb[map[n].ofs],
				       sql_est - m - 4);
			m += strlen(sql + m);
			sql[m++] = '\'';
			break;
		case LSMT_STRING_PTR:
			p = *((const char * const *)&stb[map[n].ofs]);
			sql[m++] = '\'';
			lws_sql_purify(sql + m, p, sql_est - m - 4);
			m += strlen(sql + m);
			sql[m++] = '\'';
			break;
		default:
			lwsl_err("%s: unsupported type\n", __func__);
			assert(0);
			break;
		}

		if (n != nentries - 1) {
			if (sql_est - m < 6)
				return -1;
			sql[m++] = ',';
			sql[m++] = ' ';
		}
	}

	lws_snprintf(sql + m, sql_est - m, ");");

	n = sqlite3_exec(pdb, sql, NULL, NULL, NULL);
	free(sql);
	if (n != SQLITE_OK) {
		lwsl_err("%s: %s: fail\n", __func__, sqlite3_errmsg(pdb));
		return -1;
	}

	return 0;
}

int
lws_struct_sq3_serialize(sqlite3 *pdb, const lws_struct_map_t *schema,
			 lws_dll2_owner_t *owner)
{
	uint32_t idx = 0;

	lws_start_foreach_dll(struct lws_dll2 *, p, owner->head) {
		void *item = (void *)((uint8_t *)p - schema->ofs_clist);
		if (_lws_struct_sq3_ser_one(pdb, schema, idx++, item))
			return 1;

	} lws_end_foreach_dll(p);

	return 0;
}

int
lws_struct_sq3_create_table(sqlite3 *pdb, const lws_struct_map_t *schema)
{
	const lws_struct_map_t *map = schema->child_map;
	int map_size = schema->child_map_size, subsequent = 0;
	char s[2048], *p = s, *end = &s[sizeof(s) - 1], *pri = " primary key";

	p += lws_snprintf(p, end - p,
			  "create table if not exists %s (_lws_idx integer, ",
			  schema->colname);

	while (map_size--) {
		if (map->type > LSMT_STRING_PTR) {
			map++;
			continue;
		}
		if (subsequent && (end - p) > 4) {
			*p++ = ',';
			*p++ = ' ';
		}
		subsequent = 1;
		if (map->type < LSMT_STRING_CHAR_ARRAY)
			p += lws_snprintf(p, end - p, "%s integer%s",
					  map->colname, pri);
		else
			p += lws_snprintf(p, end - p, "%s varchar%s",
					  map->colname, pri);
		pri = "";
		map++;
	}

	p += lws_snprintf(p, end - p, ");");

	if (sqlite3_exec(pdb, s, NULL, NULL, NULL) != SQLITE_OK) {
		lwsl_err("%s: %s: fail\n", __func__, sqlite3_errmsg(pdb));

		return -1;
	}

	return 0;
}

int
lws_struct_sq3_open(struct lws_context *context, const char *sqlite3_path,
		    sqlite3 **pdb)
{
	int uid = 0, gid = 0;

	if (sqlite3_open_v2(sqlite3_path, pdb,
			    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
			    NULL) != SQLITE_OK) {
		lwsl_err("%s: Unable to open db %s: %s\n",
			 __func__, sqlite3_path, sqlite3_errmsg(*pdb));

		return 1;
	}

	lws_get_effective_uid_gid(context, &uid, &gid);
	if (uid)
		chown(sqlite3_path, uid, gid);
	chmod(sqlite3_path, 0600);

	lwsl_notice("%s: created %s owned by %u:%u mode 0600\n", __func__,
			sqlite3_path, (unsigned int)uid, (unsigned int)gid);

	sqlite3_extended_result_codes(*pdb, 1);

	return 0;
}

int
lws_struct_sq3_close(sqlite3 **pdb)
{
	if (!*pdb)
		return 0;

	sqlite3_close(*pdb);
	*pdb = NULL;

	return 0;
}
