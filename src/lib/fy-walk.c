/*
 * fy-walk.c - path walker
 *
 * Copyright (c) 2021 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-doc.h"
#include "fy-walk.h"

#include "fy-utils.h"

/* NOTE that walk results do not take references and it is invalid to
 * use _any_ call that modifies the document structure
 */
struct fy_walk_result *fy_walk_result_alloc(void)
{
	struct fy_walk_result *fwr = NULL;

	fwr = malloc(sizeof(*fwr));
	if (!fwr)
		return NULL;
	memset(fwr, 0, sizeof(*fwr));
	return fwr;
}

void fy_walk_result_free(struct fy_walk_result *fwr)
{
	if (!fwr)
		return;
	free(fwr);
}

void fy_walk_result_list_free(struct fy_walk_result_list *results)
{
	struct fy_walk_result *fwr;

	while ((fwr = fy_walk_result_list_pop(results)) != NULL)
		fy_walk_result_free(fwr);
}

struct fy_walk_component *fy_walk_component_alloc(void)
{
	struct fy_walk_component *fwc = NULL;

	fwc = malloc(sizeof(*fwc));
	if (!fwc)
		return NULL;
	memset(fwc, 0, sizeof(*fwc));
	fy_walk_component_list_init(&fwc->children);

	return fwc;
}

void fy_walk_component_free(struct fy_walk_component *fwc)
{
	struct fy_walk_component *fwcn;

	if (!fwc)
		return;

	while ((fwcn = fy_walk_component_list_pop(&fwc->children)) != NULL)
		fy_walk_component_free(fwcn);

	switch (fwc->type) {
	case fwct_map_key:
		if (fwc->map_key.fyd)
			fy_document_destroy(fwc->map_key.fyd);
		break;
	default:
		break;
	}

	free(fwc);
}

void fy_walk_destroy(struct fy_walk_ctx *wc)
{
	struct fy_walk_component *fwc;

	if (!wc)
		return;

	while ((fwc = fy_walk_component_list_pop(&wc->components)) != NULL)
		fy_walk_component_free(fwc);

	if (wc->path)
		free(wc->path);

	free(wc);
}

static bool walk_container_is_startc(int c)
{
	return c == '"' || c == '\'' ||
#if 0
		c == '(' ||
#endif
		c == '{' || c == '[';
}

static int walk_container_endc(int c)
{
	if (c == '"' || c == '\'')
		return c;
#if 0
	if (c == '(')
		return ')';
#endif
	if (c == '{')
		return '}';
	if (c == '[')
		return ']';
	return -1;
}

static int walk_container_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *e = s + len;
	const char *t;
	int c, startc, endc, escc, nest, ret;
	enum fy_utf8_escape esc;
	size_t rlen;

	/* not a container, no problemo */
	if (!len || !walk_container_is_startc(*s)) {
		*next = s;
		return 0;
	}
	
	c = *s++;
	startc = c;
	endc = walk_container_endc(startc);

	if (startc == '"' || startc == '\'') {

		if (startc == '\'') {
			escc = '\'';
			esc = fyue_singlequote;
		} else {
			escc = '\\';
			esc = fyue_doublequote;
		}

		while (s < e) {
			/* find next escape */
			t = s;
			while (t < e) {
				if (*t == escc || *t == endc)
					break;
				t++;
			}
			if (t >= e)		/* end of string without finding anything */
				return -1;

			rlen = (t ? t : e) - s;
			s += rlen;

			/* end of string */
			if (*t == endc && endc != escc)
				break;

			/* get and skip over escape */
			ret = fy_utf8_parse_escape(&t, e - t, esc);
			if (ret < 0) {
				/* bad escape if escc != escc */
				if (endc != escc)
					return -1;
				/* regular end of container */
				break;
			}
			s = t;
		}

	} else {
		nest = 1;
		while (s < e) {
			if (*s == startc)
				nest++;
			else if (*s == endc && --nest == 0)
				break;
			s++;
		}
		if (nest != 0)
			return -1;
	}

	/* end without finding matching container close */
	if (s >= e) {
		return -1;
	}
	c = *s++;

	/* end but not with matching ending character */
	if (c != endc)
		return -1;

	*next = s;
	return 0;
}

static int walk_numeric_slice_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *ss = s;
	const char *e = s + len;
	const char *t;

	if (!len)
		goto ok;

	/* slices are always zero or positive */

	t = s;
	while (s < e && isdigit(*s))
		s++;

	/* no digits consumed at all? */
	if (t == s) {
		s = ss;
		goto ok;
	}

	/* a numeric slice must exist */
	if (s >= e || *s != ':') {
		s = ss;
		goto ok;
	}
	s++;

	/* no second range (marks end of sequence) */
	if (s >= e)
		goto ok;

	t = s;
	while (s < e && isdigit(*s))
		s++;

	/* no digits consumed at all? */
	if (t == s) {
		s = ss;
		goto ok;
	}

ok:
	*next = s;
	return 0;
}

static int walk_numeric_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *ss = s;
	const char *e = s + len;
	const char *t;

	if (!len)
		goto ok;

	/* skip sign */
	if (*s == '-')
		s++;

	/* nothing else afterwards? */
	if (s >= e) {
		s = ss;
		goto ok;
	}

	t = s;
	while (s < e && isdigit(*s))
		s++;

	/* no digits consumed at all? */
	if (t == s) {
		s = ss;
		goto ok;
	}

ok:
	*next = s;
	return 0;
}

static int walk_simple_key_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *ss = s;
	const char *e = s + len;

	if (!len)
		goto ok;

	/* any of those is not a valid simple key */
	if (strchr(",[]{}#&*!|<>'\"%@`?:/$-0123456789", *s)) {
		fy_notice(diag, "%s: invalid first character\n", __func__);
		goto ok;
	}

	s++;
	while (s < e && !strchr(",[]{}#&*!|<>'\"%@`?:/$", *s))
		s++;

	fy_notice(diag, "%s: got simple key %.*s\n", __func__, (int)(s - ss), ss);
ok:
	*next = s;
	return 0;
}

static int walk_alias_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *ss = s;
	const char *e = s + len;

	if (!len)
		goto ok;

	/* regular non instane alias only */
	if ((e - s) < 2 || s[0] != '*' || !fy_is_first_alpha(s[1]))
		goto ok;

	s++;
	ss = s;
	while (s < e && fy_is_alpha(*s))
		s++;

	fy_notice(diag, "%s: got alias %.*s\n", __func__, (int)(s - ss), ss);
ok:
	*next = s;
	return 0;
}

static int walk_parent_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *e = s + len;

	if (len < 2 || s[0] != '.' || s[1] != '.')
		goto ok;

	s += 2;
	if (s < e && !strchr(",/", *s))
		return -1;
ok:
	*next = s;
	return 0;
}

static int walk_this_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *e = s + len;

	if (len < 1 || s[0] != '.')
		goto ok;

	s++;
	if (s < e && !strchr(",/", *s))
		return -1;
ok:
	*next = s;
	return 0;
}

static int walk_root_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *e = s + len;

	if (len < 1 || s[0] != '^')
		goto ok;

	s++;
	if (s < e && !strchr(",/", *s))
		return -1;
ok:
	*next = s;
	return 0;
}

static int walk_every_child_r_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	if (len < 2 || s[0] != '*' || s[1] != '*')
		goto ok;

	s += 2;
ok:
	*next = s;
	return 0;
}

static int walk_every_child_get_extent(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const char *e = s + len;

	if (len < 1 || s[0] != '*')
		goto ok;

	s++;
	if (s < e && !strchr(",/", *s))
		return -1;
ok:
	*next = s;
	return 0;
}

struct split_desc {
	const char *name;
	enum fy_walk_component_type ctype;
	int (*get_extent)(const char *s, size_t len, const char **next, struct fy_diag *diag);
	bool sibling_mark;
	bool scalar_mark;
};

struct split {
	const struct split_desc *sd;
	const char *s;
	size_t len;
	bool sibling_mark;
	bool scalar_mark;
	bool collection_mark;
	bool mapping_mark;
	bool sequence_mark;
};

static const struct split_desc split_descs[] = {
	/* in order of precedence */
	{
		.name		= "root",
		.ctype		= fwct_root,
		.get_extent	= walk_root_get_extent,
	}, {
		.name		= "parent",
		.ctype		= fwct_parent,
		.get_extent	= walk_parent_get_extent,
	}, {
		.name		= "this",
		.ctype		= fwct_this,
		.get_extent	= walk_this_get_extent,
	}, {
		.name		= "numeric-slice",
		.ctype		= fwct_seq_slice,
		.get_extent	= walk_numeric_slice_get_extent,
		.sibling_mark	= true,
		.scalar_mark	= true,
	}, {
		.name		= "numeric",
		.ctype		= fwct_seq_index,
		.get_extent	= walk_numeric_get_extent,
		.sibling_mark	= true,
		.scalar_mark	= true,
	}, {
		.name		= "container",
		.ctype		= fwct_map_key,
		.get_extent	= walk_container_get_extent,
		.sibling_mark	= true,
		.scalar_mark	= true,
	}, {
		.name		= "simple-key",
		.ctype		= fwct_simple_map_key,
		.get_extent	= walk_simple_key_get_extent,
		.sibling_mark	= true,
		.scalar_mark	= true,
	}, {
		.name		= "alias",
		.ctype		= fwct_start_alias,
		.get_extent	= walk_alias_get_extent,
		.scalar_mark	= true,
	}, {
		.name		= "every-child-recursive",
		.ctype		= fwct_every_child_r,
		.get_extent	= walk_every_child_r_get_extent,
		.scalar_mark	= true,
	}, {
		.name		= "every-child",
		.ctype		= fwct_every_child,
		.get_extent	= walk_every_child_get_extent,
		.scalar_mark	= true,
	},
};

static const struct split_desc *
walk_get_split_desc(const char *s, size_t len, const char **next, struct fy_diag *diag)
{
	const struct split_desc *sd;
	const char *e = s + len, *t;
	unsigned int i;
	int ret;

	fy_notice(diag, "%s: checking %.*s\n", __func__, (int)len, s);

	for (i = 0, sd = split_descs; i < sizeof(split_descs)/sizeof(split_descs[0]); i++, sd++) {

		fy_notice(diag, "%s: checking against type %s\n", __func__, sd->name);
		/* it's a container double quoted, single quoted, parentheses, flow seq or flow map */
		ret = sd->get_extent(s, e - s, &t, diag);
		if (ret)
			return NULL;

		/* if we advanced we found it */
		if (t > s) {
			*next = t;
			return sd;
		}
	}

	return NULL;
}

const char *walk_component_type_txt[] = {
	[fwct_none]			= "none",
	/* */
	[fwct_start_root]		= "start-root",
	[fwct_start_alias]		= "start-alias",
	/* */
	[fwct_root]			= "root",
	[fwct_this]			= "this",
	[fwct_parent]			= "parent",
	[fwct_every_child]		= "every-child",
	[fwct_every_child_r]		= "every-child-recursive",
	[fwct_every_leaf]		= "every-leaf",
	[fwct_assert_collection]	= "assert-collection",
	[fwct_assert_scalar]		= "assert-scalar",
	[fwct_assert_sequence]		= "assert-sequence",
	[fwct_assert_mapping]		= "assert-mapping",
	[fwct_simple_map_key]		= "simple-map-key",
	[fwct_seq_index]		= "seq-index",
	[fwct_seq_slice]		= "seq-slice",

	[fwct_map_key]			= "map-key",

	[fwct_multi]			= "multi",
	[fwct_chain]			= "chain",
};

struct fy_walk_component *
fy_walk_add_component(struct fy_walk_ctx *wc, struct fy_diag *diag,
		      struct fy_walk_component *parent,
      		      enum fy_walk_component_type type, const char *start, size_t len, ...)
{
	struct fy_walk_component_list *list;
	struct fy_walk_component *fwc = NULL;
	const char *s, *e;
	char *buf, *end_idx;
	va_list ap;

	if (!wc || !fy_walk_component_type_is_valid(type))
		return NULL;

	fy_notice(diag, "%s: %.*s\n", __func__, (int)len, start);

	s = start;
	e = s + len;
	assert(s >= wc->path && s < wc->path + wc->pathlen);
	assert(e >= wc->path && e <= wc->path + wc->pathlen);

	fwc = fy_walk_component_alloc();
	if (!fwc) {
		fy_error(diag, "%s: fy_walk_component_alloc() failed\n", __func__);
		goto err_out;
	}
	fwc->parent = parent;
	fwc->wc = wc;
	fwc->type = type;
	fwc->comp = start;
	fwc->complen = len;

	va_start(ap, len);
	switch (type) {
	case fwct_start_alias:
		assert(len > 1);
		fwc->alias.alias = start + 1;
		fwc->alias.aliaslen = len - 1;
		break;

	case fwct_map_key:
		fwc->map_key.fyd = fy_document_build_from_string(NULL, start, len);
		if (!fwc->map_key.fyd) {
			fy_error(diag, "%s: fy_document_build_from_string() failed\n", __func__);
			goto err_out;
		}
		break;
	case fwct_seq_index:
		buf = alloca(len + 1);
		memcpy(buf, start, len);
		buf[len] = '\0';
		fwc->seq_index.index = (int)strtol(buf, &end_idx, 10);
		/* everything must be consumed */
		if (*end_idx != '\0') {
			fy_error(diag, "%s: garbage after numeric\n", __func__);
			goto err_out;
		}
		break;

	case fwct_seq_slice:
		buf = alloca(len + 1);
		memcpy(buf, start, len);
		buf[len] = '\0';
		fwc->seq_slice.start_index = (int)strtol(buf, &end_idx, 10);
		/* everything must be consumed */
		if (*end_idx != ':') {
			fy_error(diag, "%s: garbage after first slice index\n", __func__);
			goto err_out;
		}
		if (fwc->seq_slice.start_index < 0) {
			fy_error(diag, "%s: bad sequence slice start index\n", __func__);
			goto err_out;
		}
		end_idx++;
		if (*end_idx != '\0') {
			fwc->seq_slice.end_index = (int)strtol(end_idx, &end_idx, 10);
			if (*end_idx != '\0') {
				fy_error(diag, "%s: garbage after second slice index\n", __func__);
				goto err_out;
			}
			if (fwc->seq_slice.end_index < 0 || fwc->seq_slice.start_index >= fwc->seq_slice.end_index) {
				fy_error(diag, "%s: bad end sequence slice end index\n", __func__);
				goto err_out;
			}
		} else {
			fwc->seq_slice.end_index = -1;
		}
		break;

	default:
		/* nothing extra for those */
		break;
	}
	va_end(ap);

	list = parent ? &parent->children : &wc->components;
	fy_walk_component_list_add_tail(list, fwc);

	fy_notice(diag, "%s: added component %s: %.*s\n", __func__,
			walk_component_type_txt[type], (int)len, start);

	return fwc;

err_out:
	fy_walk_component_free(fwc);
	return NULL;
}

struct fy_walk_ctx *
fy_walk_create(const char *path, size_t len, struct fy_diag *diag)
{
	struct fy_walk_ctx *wc = NULL;
	struct fy_walk_component *fwc, *fwc_parent, *fwc_chain, *fwc_expr_parent;
	const char *s, *e, *ss, *t;
	bool sibling_mark, scalar_mark;
	const struct split_desc *sd;
	unsigned int i, split_count, split_alloc;
	struct split *splits, *splitse, *split;
	int c;

	if (!path || !len) {
		fy_error(diag, "%s: path empty\n", __func__);
		goto err_out;
	}

	wc = malloc(sizeof(*wc));
	if (!wc) {
		fy_error(diag, "%s: unable to allocate wc\n", __func__);
		goto err_out;
	}

	if (len == (size_t)-1)
		len = strlen(path);

	/* strip leading and trailing spaces */
	s = path;
	e = path + len;
	while (s < e && isspace(*s))
		s++;
	while (s < e && isspace(e[-1]))
		e--;
	path = s;
	len = e - s;

	/* nothing but spaces huh? */
	if (!len) {
		fy_error(diag, "%s: path empty (2)\n", __func__);
		goto err_out;
	}

	memset(wc, 0, sizeof(*wc));
	fy_walk_component_list_init(&wc->components);

	wc->path = malloc(len + 1);
	if (!wc->path) {
		fy_error(diag, "%s: unable to allocate copy of path\n", __func__);
		goto err_out;
	}
	memcpy(wc->path, path, len);
	wc->path[len] = '\0';
	wc->pathlen = len;

	s = wc->path;
	e = s + len;

	/* count the commas in the path, we can't have more than n + 1 splits */
	ss = s;
	split_alloc = 0;
	while (ss < e) {
		if (*ss == ',')
			split_alloc++;
		ss++;
	}
	split_alloc++;
	split_count = 0;
	splits = alloca(sizeof(*splits) * split_alloc);
	splitse = splits + split_alloc;

	assert(e > s && len > 0);

	while (s < e) {

		/* regular path follows (and no previous anchor) */
		if (*s == '/' && fy_walk_component_list_empty(&wc->components)) {

			/* if the component list is empty, start from root */

			fwc = fy_walk_add_component(wc, diag, NULL, fwct_start_root, s, 1);
			if (!fwc) {
				fy_error(diag, "fy_walk_add_component() failed\n");
				goto err_out;
			}
			if (++s >= e)
				goto done;
		}

		/* terminating / */
		if (*s == '/' && (e - s) <= 1) {
			fwc = fy_walk_add_component(wc, diag, NULL, fwct_assert_collection, s, 1);
			if (!fwc) {
				fy_error(diag, "fy_walk_add_component() failed\n");
				goto err_out;
			}
			s++;
			goto done;
		}

		/* mark start */
		ss = s;

		split = splits;
		split_count = 0;
		while (s < e) {

			c = *s;

			sibling_mark = false;
			scalar_mark = false;

			/* :<> is a sibling mark */
			if ((e - s) >= 1 && c == ':') {
				sibling_mark = true;
				s++;
				c = *s;
			}

			sd = walk_get_split_desc(s, e - s, &t, diag);
			if (!sd) {
				/* everything failed */
				fy_error(diag, "%s: could not split to advance at all %.*s\n", __func__,
						(int)(e - s), s);
				goto err_out;
			}

			if (sibling_mark && !sd->sibling_mark) {
				fy_error(diag, "%s: component does not support sibling mark %.*s\n", __func__,
						(int)(e - s), s);
				goto err_out;
			}

			assert(split < splitse);
			memset(split, 0, sizeof(*split));
			split->sd = sd;
			split->s = s;
			split->len = t - s;
			split->sibling_mark = sibling_mark;

			fy_notice(diag, "%s: split %.*s\n", __func__,
					(int)(t - s), s);

			scalar_mark = false;
			if (t < e && *t == '$') {
				scalar_mark = true;
				t++;
			}

			split->scalar_mark = scalar_mark;

			if (scalar_mark && !sd->scalar_mark) {
				fy_error(diag, "%s: component does not support leaf mark %.*s\n", __func__,
						(int)(e - s), s);
				goto err_out;
			}

			s = t;
			len = e - s;

			split++;
			split_count++;

			/* we're done */
			if (s >= e)
				break;

			if (*s == '/') {
				s++;
				break;
			}

			/* split or */
			if (*s == ',') {
				s++;
				continue;
			}

			/* no end, comma, or slash */
			fy_error(diag, "%s: no end, comma or slash found %.*s\n", __func__,
					(int)(e - s), s);
			goto err_out;
		}

		len = e - s;

		/* no split count is an error */
		if (!split_count) {
			fy_error(diag, "%s: no splits found\n", __func__);
			goto err_out;
		}

		fwc_parent = NULL;
		if (split_count > 1) {
			fwc_parent = fy_walk_add_component(wc, diag, NULL, fwct_multi, ss, 0);
			if (!fwc_parent) {
				fy_error(diag, "fy_walk_add_component() failed\n");
				goto err_out;
			}
		}

		
		for (split = splits, i = 0; i < split_count; i++, split++) {

			fwc_chain = NULL;
			fwc_expr_parent = fwc_parent;

			if (split->sibling_mark || split->scalar_mark) {
				fwc_chain = fy_walk_add_component(wc, diag, fwc_parent, fwct_chain, split->s, 0);
				if (!fwc_chain) {
					fy_error(diag, "fy_walk_add_component() failed\n");
					goto err_out;
				}
				fwc_expr_parent = fwc_chain;
			}

			if (split->sibling_mark) {
				fwc = fy_walk_add_component(wc, diag, fwc_expr_parent, fwct_parent, split->s, 0);
				if (!fwc) {
					fy_error(diag, "fy_walk_add_component() failed\n");
					goto err_out;
				}
			}

			fwc = fy_walk_add_component(wc, diag, fwc_expr_parent, split->sd->ctype, split->s, split->len);
			if (!fwc) {
				fy_error(diag, "fy_walk_add_component() failed\n");
				goto err_out;
			}

			if (split->scalar_mark) {
				fwc = fy_walk_add_component(wc, diag, fwc_expr_parent, fwct_assert_scalar, split->s + split->len, 0);
				if (!fwc) {
					fy_error(diag, "fy_walk_add_component() failed\n");
					goto err_out;
				}
			}

		}

		/* terminating component with more remaining is illegal */
		fwc = fy_walk_component_list_tail(&wc->components);
		if (fwc && fy_walk_component_type_is_terminating(fwc->type) && s < e) {
			fy_error(diag, "%s: terminating component with more remaining is illegal\n", __func__);
			goto err_out;
		}

	}

done:

	if (fy_walk_component_list_empty(&wc->components)) {
		fy_error(diag, "%s: no components discovered error\n", __func__);
		goto err_out;
	}

	fy_notice(diag, "%s: OK\n", __func__);

	return wc;

err_out:
	fy_walk_destroy(wc);	/* NULL is fine */
	return NULL;
}

int fy_walk_result_add(struct fy_walk_result_list *results, struct fy_node *fyn)
{
	struct fy_walk_result *fwr;

	/* do not add multiple times */
	for (fwr = fy_walk_result_list_head(results); fwr; fwr = fy_walk_result_next(results, fwr)) {
		if (fwr->fyn == fyn)
			return 0;
	}

	fwr = fy_walk_result_alloc();
	if (!fwr) {
		fprintf(stderr, "%s:%d error\n", __FILE__, __LINE__);
		return -1;
	}
	fwr->fyn = fyn;
	fy_walk_result_list_add_tail(results, fwr);
	return 0;
}

int fy_walk_result_add_recursive(struct fy_walk_result_list *results, struct fy_node *fyn, bool leaf_only)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp;
	int ret;

	if (!fyn)
		return 0;

	if (fy_node_is_scalar(fyn))
		return fy_walk_result_add(results, fyn);

	if (!leaf_only) {
		ret = fy_walk_result_add(results, fyn);
		if (ret)
			return ret;
	}

	if (fy_node_is_sequence(fyn)) {
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {

			ret = fy_walk_result_add_recursive(results, fyni, leaf_only);
			if (ret)
				return ret;
		}
	} else {
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp;
				fynp = fy_node_pair_next(&fyn->mapping, fynp)) {

			ret = fy_walk_result_add_recursive(results, fynp->value, leaf_only);
			if (ret)
				return ret;
		}
	}
	return 0;
}

struct fy_node *
fy_walk_component_next_node_single(struct fy_walk_component *fwc, struct fy_node *fyn)
{
	struct fy_anchor *fya;

	assert(fwc);

	/* no node, do not continue */
	if (!fyn)
		return NULL;

	switch (fwc->type) {
	case fwct_start_root:
	case fwct_root:
		return fyn->fyd->root;

	case fwct_start_alias:
		fya = fy_document_lookup_anchor(fyn->fyd, fwc->alias.alias, fwc->alias.aliaslen);
		return fya ? fya->fyn : NULL;

	case fwct_this:
		return fyn;

	case fwct_parent:
		return fy_node_get_parent(fyn);

	case fwct_simple_map_key:
		if (!fy_node_is_mapping(fyn))
			return NULL;
		return fy_node_mapping_lookup_value_by_simple_key(fyn, fwc->comp, fwc->complen);

	case fwct_map_key:
		if (!fy_node_is_mapping(fyn))
			return NULL;
		return fy_node_mapping_lookup_value_by_key(fyn, fy_document_root(fwc->map_key.fyd));

	case fwct_seq_index:
		if (!fy_node_is_sequence(fyn))
			return NULL;
		return fy_node_sequence_get_by_index(fyn, fwc->seq_index.index);

	case fwct_assert_collection:
		return !fy_node_is_scalar(fyn) ? fyn : NULL;

	case fwct_assert_scalar:
		return fy_node_is_scalar(fyn) ? fyn : NULL;

	case fwct_assert_sequence:
		return fy_node_is_sequence(fyn) ? fyn : NULL;

	case fwct_assert_mapping:
		return fy_node_is_mapping(fyn) ? fyn : NULL;

		/* multiple nodes */
	case fwct_every_child:
	case fwct_every_child_r:
	case fwct_every_leaf:
	case fwct_multi:
	case fwct_chain:
	case fwct_seq_slice:
		return NULL;

	default:
		break;
	}

	return NULL;
}

struct fy_walk_component *
fy_walk_component_next_in_seq(struct fy_walk_component *fwc)
{
	if (!fwc)
		return NULL;

	if (!fwc->parent)
		return fy_walk_component_next(&fwc->wc->components, fwc);

	switch (fwc->parent->type) {
	case fwct_multi:
		return fy_walk_component_next_in_seq(fwc->parent);

	default:
		return fy_walk_component_next(&fwc->parent->children, fwc);

	}

	return NULL;
}

static int
fy_walk_perform_internal(struct fy_walk_ctx *wc,
			 struct fy_walk_result_list *results,
			 struct fy_node *fyn,
			 struct fy_walk_component *fwc)
{
	struct fy_walk_component *fwcn;
	struct fy_node *fynn;
	struct fy_walk_result *fwr;
	struct fy_walk_result_list tresults;
	struct fy_node *fyni;
	struct fy_node_pair *fynp;
	int ret, rret, start_index, end_index, i;

	/* no node, do not continue */
	if (!fyn)
		return 0;

	fprintf(stderr, "%s: at %s\n", __func__, fy_node_get_path_alloca(fyn));

	/* we can iterate on the single component (and avoid recursion overhead) */
	while (fwc && !fy_walk_component_type_is_multi(fwc->type)) {

		fprintf(stderr, "%s: single '%s' before %s\n", __func__,
				walk_component_type_txt[fwc->type], fy_node_get_path_alloca(fyn));

		fyn = fy_walk_component_next_node_single(fwc, fyn);
		if (!fyn) {
			fprintf(stderr, "%s: single '%s' NULL\n", __func__,
					walk_component_type_txt[fwc->type]);
			return 0;
		}

		fprintf(stderr, "%s: single '%s' after %s (1)\n", __func__,
				walk_component_type_txt[fwc->type], fy_node_get_path_alloca(fyn));

		fwc = fy_walk_component_next_in_seq(fwc);

		fprintf(stderr, "%s: single '%s' after %s (2)\n", __func__,
				fwc ? walk_component_type_txt[fwc->type] : "NULL", fy_node_get_path_alloca(fyn));
	}

	/* no next component? add node */
	if (!fwc) {
		fprintf(stderr, "%s: adding result %s\n", __func__, fy_node_get_path_alloca(fyn));
		return fy_walk_result_add(results, fyn);
	}

	fprintf(stderr, "%s: multi '%s' start %s\n", __func__,
			walk_component_type_txt[fwc->type], fy_node_get_path_alloca(fyn));

	/* sanity checking */
	assert(fyn);
	assert(fy_walk_component_type_is_multi(fwc->type));

	switch (fwc->type) {
	case fwct_every_child:

		if (fy_node_is_scalar(fyn))
			return fy_walk_result_add(results, fyn);

		rret = 0;
		if (fy_node_is_sequence(fyn)) {
			for (fyni = fy_node_list_head(&fyn->sequence); fyni;
					fyni = fy_node_next(&fyn->sequence, fyni)) {

				ret = fy_walk_perform_internal(wc, results, fyni, fwc);
				if (!rret && ret)
					rret = ret;
				if (ret)
					break;

			}
		} else {
			for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp;
					fynp = fy_node_pair_next(&fyn->mapping, fynp)) {

				ret = fy_walk_perform_internal(wc, results, fynp->value, fwc);
				if (!rret && ret)
					rret = ret;
				if (ret)
					break;
			}
		}
		return rret;

		/* terminating */
	case fwct_every_child_r:
	case fwct_every_leaf:

		fwcn = fy_walk_component_next_in_seq(fwc);

		if (!fwcn)
			return fy_walk_result_add_recursive(results, fyn, fwc->type == fwct_every_leaf);

		fy_walk_result_list_init(&tresults);

		ret = fy_walk_result_add_recursive(&tresults, fyn, fwc->type == fwct_every_leaf);
		if (ret) {
			fy_walk_result_list_free(&tresults);
			return ret;
		}

		/* next component, but starting from the results */
		rret = 0;
		while ((fwr = fy_walk_result_list_pop(&tresults)) != NULL) {

			fprintf(stderr, "%s: and result @%s\n", __func__, fy_node_get_path_alloca(fwr->fyn));

			ret = fy_walk_perform_internal(wc, results, fwr->fyn, fwcn);

			fy_walk_result_free(fwr);

			if (!rret && ret)
				rret = ret;
			if (ret)
				break;
		}

		if (rret)
			return rret;

		return 0;
		
	case fwct_seq_slice:

		if (!fy_node_is_sequence(fyn))
			return 0;

		start_index = fwc->seq_slice.start_index;
		end_index = fwc->seq_slice.end_index;

		if (end_index == -1)
			end_index = fy_node_sequence_item_count(fyn);

		/* if it's invalid bolt out */
		if (start_index < 0 || end_index < 0 || start_index >= end_index)
			return 0;

		/* if the start index is off range, don't bother */
		if (start_index >= fy_node_sequence_item_count(fyn))
			return 0;

		fwcn = fy_walk_component_next_in_seq(fwc);

		rret = 0;
		for (i = start_index; i < end_index; i++) {
			fynn = fy_node_sequence_get_by_index(fyn, i);
			if (!fynn)
				continue;

			ret = fy_walk_perform_internal(wc, results, fynn, fwcn);

			if (!rret && ret)
				rret = ret;
			if (ret)
				break;
		}

		if (rret)
			return rret;

		return 0;

	case fwct_multi:
		/* go down */

		rret = 0;
		for (fwcn = fy_walk_component_list_head(&fwc->children); fwcn;
				fwcn = fy_walk_component_next(&fwc->children, fwcn)) {

			ret = fy_walk_perform_internal(wc, results, fyn, fwcn);
			if (!rret && ret)
				rret = ret;
			if (ret)
				break;
		}
		if (rret)
			return rret;

		return 0;

	case fwct_chain:

		fy_walk_result_list_init(&tresults);

		ret = fy_walk_perform_internal(wc, &tresults, fyn, fy_walk_component_list_head(&fwc->children));
		if (ret) {
			fy_walk_result_list_free(&tresults);
			return ret;
		}

		/* if no next component, all are results */
		fwcn = fy_walk_component_next_in_seq(fwc);
		if (!fwcn) {
			while ((fwr = fy_walk_result_list_pop(&tresults)) != NULL) {
				fy_walk_result_add(results, fwr->fyn);
				fy_walk_result_free(fwr);
			}
			return 0;
		}

		/* next component, but starting from the results */
		rret = 0;
		while ((fwr = fy_walk_result_list_pop(&tresults)) != NULL) {

			fprintf(stderr, "%s: and result @%s\n", __func__, fy_node_get_path_alloca(fwr->fyn));

			ret = fy_walk_perform_internal(wc, results, fwr->fyn, fwcn);

			fy_walk_result_free(fwr);

			if (!rret && ret)
				rret = ret;
			if (ret)
				break;
		}

		if (rret)
			return rret;

		return 0;

	default:
		break;
	}

	return 0;
}

int fy_walk_perform(struct fy_walk_ctx *wc, struct fy_walk_result_list *results, struct fy_node *fyn)
{
	struct fy_walk_component *fwc;

	if (!wc || !results || !fyn)
		return -1;

	fwc = fy_walk_component_list_head(&wc->components);
	if (!fwc)
		return -1;

	return fy_walk_perform_internal(wc, results, fyn, fwc);
}
