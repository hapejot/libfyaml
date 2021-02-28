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

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(x) ((sizeof(x)/sizeof((x)[0])))
#endif

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

////////////////////////////////////////////////////////////////////////////////////////////

struct fy_path_expr *fy_path_expr_alloc(void)
{
	struct fy_path_expr *expr = NULL;

	expr = malloc(sizeof(*expr));
	if (!expr)
		return NULL;
	memset(expr, 0, sizeof(*expr));
	fy_path_expr_list_init(&expr->children);

	return expr;
}

void fy_path_expr_free(struct fy_path_expr *expr)
{
	struct fy_path_expr *exprn;

	if (!expr)
		return;

	while ((exprn = fy_path_expr_list_pop(&expr->children)) != NULL)
		fy_path_expr_free(exprn);

	fy_token_unref(expr->fyt);

	free(expr);
}

struct fy_path_expr *fy_path_expr_alloc_recycle(struct fy_path_parser *fypp)
{
	struct fy_path_expr *expr;

	if (!fypp || fypp->suppress_recycling)
		return fy_path_expr_alloc();

	expr = fy_path_expr_list_pop(&fypp->expr_recycle);
	if (expr)
		return expr;

	return fy_path_expr_alloc();
}

void fy_path_expr_free_recycle(struct fy_path_parser *fypp, struct fy_path_expr *expr)
{
	struct fy_path_expr *exprn;

	if (!fypp || fypp->suppress_recycling) {
		fy_path_expr_free(expr);
		return;
	}
		
	while ((exprn = fy_path_expr_list_pop(&expr->children)) != NULL)
		fy_path_expr_free_recycle(fypp, exprn);

	if (expr->fyt) {
		fy_token_unref(expr->fyt);
		expr->fyt = NULL;
	}
	fy_path_expr_list_add_tail(&fypp->expr_recycle, expr);
}

const char *path_expr_type_txt[] = {
	[fpet_none]			= "none",
	/* */
	[fpet_root]			= "root",
	[fpet_this]			= "this",
	[fpet_parent]			= "parent",
	[fpet_every_child]		= "every-child",
	[fpet_every_child_r]		= "every-child-recursive",
	[fpet_every_leaf]		= "every-leaf",
	[fpet_assert_collection]	= "assert-collection",
	[fpet_assert_scalar]		= "assert-scalar",
	[fpet_assert_sequence]		= "assert-sequence",
	[fpet_assert_mapping]		= "assert-mapping",
	[fpet_simple_map_key]		= "simple-map-key",
	[fpet_seq_index]		= "seq-index",
	[fpet_seq_slice]		= "seq-slice",
	[fpet_alias]			= "alias",

	[fpet_map_key]			= "map-key",

	[fpet_multi]			= "multi",
	[fpet_chain]			= "chain",
};

static struct fy_diag *fy_path_parser_reader_get_diag(struct fy_reader *fyr)
{
	struct fy_path_parser *fypp = container_of(fyr, struct fy_path_parser, reader);
	return fypp->diag;
}

static const struct fy_reader_ops fy_path_parser_reader_ops = {
	.get_diag = fy_path_parser_reader_get_diag,
};

void fy_path_parser_setup(struct fy_path_parser *fypp, struct fy_diag *diag)
{
	if (!fypp)
		return;

	memset(fypp, 0, sizeof(*fypp));
	fypp->diag = diag;
	fy_reader_setup(&fypp->reader, &fy_path_parser_reader_ops);
	fy_token_list_init(&fypp->queued_tokens);

	/* use the static stack at first, faster */
	fypp->operators = fypp->operators_static;
	fypp->operands = fypp->operands_static;

	fypp->operator_alloc = ARRAY_SIZE(fypp->operators_static);
	fypp->operand_alloc = ARRAY_SIZE(fypp->operands_static);

	fy_path_expr_list_init(&fypp->expr_recycle);
	fypp->suppress_recycling = !!getenv("FY_VALGRIND");
}

void fy_path_parser_cleanup(struct fy_path_parser *fypp)
{
	struct fy_path_expr *expr;
	struct fy_token *fyt;

	if (!fypp)
		return;

	while (fypp->operator_top > 0) {
		fyt = fypp->operators[--fypp->operator_top];
		fypp->operators[fypp->operator_top] = NULL;
		fy_token_unref(fyt);
	}

	if (fypp->operators != fypp->operators_static)
		free(fypp->operators);
	fypp->operators = fypp->operators_static;
	fypp->operator_alloc = ARRAY_SIZE(fypp->operators_static);

	while (fypp->operand_top > 0) {
		expr = fypp->operands[--fypp->operand_top];
		fypp->operands[fypp->operand_top] = NULL;
		fy_path_expr_free(expr);
	}

	if (fypp->operands != fypp->operands_static)
		free(fypp->operands);
	fypp->operands = fypp->operands_static;
	fypp->operand_alloc = ARRAY_SIZE(fypp->operands_static);

	fy_reader_cleanup(&fypp->reader);
	fy_token_list_unref_all(&fypp->queued_tokens);

	while ((expr = fy_path_expr_list_pop(&fypp->expr_recycle)) != NULL)
		fy_path_expr_free(expr);

}

int fy_path_parser_open(struct fy_path_parser *fypp, 
			struct fy_input *fyi, const struct fy_reader_input_cfg *icfg)
{
	if (!fypp)
		return -1;

	return fy_reader_input_open(&fypp->reader, fyi, icfg);
}

void fy_path_parser_close(struct fy_path_parser *fypp)
{
	if (!fypp)
		return;

	fy_reader_input_done(&fypp->reader);
}

struct fy_token *fy_path_token_vqueue(struct fy_path_parser *fypp, enum fy_token_type type, va_list ap)
{
	struct fy_token *fyt;

	fyt = fy_token_list_vqueue(&fypp->queued_tokens, type, ap);
	if (fyt)
		fypp->token_activity_counter++;
	return fyt;
}

struct fy_token *fy_path_token_queue(struct fy_path_parser *fypp, enum fy_token_type type, ...)
{
	va_list ap;
	struct fy_token *fyt;

	va_start(ap, type);
	fyt = fy_path_token_vqueue(fypp, type, ap);
	va_end(ap);

	return fyt;
}

int fy_path_fetch_simple_map_key(struct fy_path_parser *fypp, int c)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	int i;

	fyr = &fypp->reader;

	/* verify that the called context is correct */
	assert(fy_is_first_alpha(c));
	i = 1;
	while (fy_is_alnum(fy_reader_peek_at(fyr, i)))
		i++;

	/* document is NULL, is a simple key */
	fyt = fy_path_token_queue(fypp, FYTT_PE_MAP_KEY, fy_reader_fill_atom_a(fyr, i), NULL);
	fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

	return 0;

err_out:
	fypp->stream_error = true;
	return -1;
}

int fy_path_fetch_seq_index_or_slice(struct fy_path_parser *fypp, int c)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	bool neg;
	int i, j, val, nval, digits, indices[2];

	fyr = &fypp->reader;

	/* verify that the called context is correct */
	assert(fy_is_num(c) || (c == '-' && fy_is_num(fy_reader_peek_at(fyr, 1))));

	i = 0;
	indices[0] = indices[1] = -1;

	j = 0;
	while (j < 2) {

		neg = false;
		if (c == '-') {
			neg = true;
			i++;
		}

		digits = 0;
		val = 0;
		while (fy_is_num((c = fy_reader_peek_at(fyr, i)))) {
			nval = (val * 10) | (c - '0');
			FYR_PARSE_ERROR_CHECK(fyr, 0, i, FYEM_SCAN,
					nval >= val && nval >= 0, err_out,
					"illegal sequence index (overflow)");
			val = nval;
			i++;
			digits++;
		}
		FYR_PARSE_ERROR_CHECK(fyr, 0, i, FYEM_SCAN,
				(val == 0 && digits == 1) || (val > 0), err_out,
				"bad number");
		if (neg)
			val = -val;

		indices[j] = val;

		/* continue only on slice : */
		if (c == ':') {
			c = fy_reader_peek_at(fyr, i + 1);
			if (fy_is_num(c) || (c == '-' && fy_is_num(fy_reader_peek_at(fyr, i + 2)))) {
				i++;
				j++;
				continue;
			}
		}

		break;
	}

	if (j >= 1)
		fyt = fy_path_token_queue(fypp, FYTT_PE_SEQ_SLICE, fy_reader_fill_atom_a(fyr, i), indices[0], indices[1]);
	else
		fyt = fy_path_token_queue(fypp, FYTT_PE_SEQ_INDEX, fy_reader_fill_atom_a(fyr, i), indices[0]);

	fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

	return 0;

err_out:
	fypp->stream_error = true;
	return -1;
}

int fy_path_fetch_flow_map_key(struct fy_path_parser *fypp, int c)
{
	struct fy_reader *fyr;
	struct fy_token *fyt;
	struct fy_document *fyd;
	struct fy_atom handle;
	struct fy_parser fyp_data, *fyp = &fyp_data;
	struct fy_parse_cfg cfg_data, *cfg = NULL;
	int rc;

	fyr = &fypp->reader;

	/* verify that the called context is correct */
	assert(fy_is_path_flow_key_start(c));

	fy_reader_fill_atom_start(fyr, &handle);

	if (fypp->diag) {
		cfg = &cfg_data;
		memset(cfg, 0, sizeof(*cfg));
		cfg->flags = fy_diag_parser_flags_from_cfg(&fypp->diag->cfg);
		cfg->diag = fypp->diag;
	} else
		cfg = NULL;

	rc = fy_parse_setup(fyp, cfg);
	fyr_error_check(fyr, !rc, err_out, "fy_parse_setup() failed\n");

	/* associate with reader and set flow mode */
	fy_parser_set_reader(fyp, fyr);
	fy_parser_set_flow_only_mode(fyp, true);

	fyd = fy_parse_load_document(fyp);

	/* cleanup the parser no matter what */
	fy_parse_cleanup(fyp);

	fyr_error_check(fyr, fyd, err_out, "fy_parse_load_document() failed\n");

	fy_reader_fill_atom_end(fyr, &handle);

	/* document is NULL, is a simple key */
	fyt = fy_path_token_queue(fypp, FYTT_PE_MAP_KEY, &handle, fyd);
	fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

	return 0;

err_out:
	fypp->stream_error = true;
	return -1;
}

int fy_path_fetch_tokens(struct fy_path_parser *fypp)
{
	enum fy_token_type type;
	struct fy_token *fyt;
	struct fy_reader *fyr;
	int c, rc, simple_token_count;

	fyr = &fypp->reader;
	if (!fypp->stream_start_produced) {

		fyt = fy_path_token_queue(fypp, FYTT_STREAM_START, fy_reader_fill_atom_a(fyr, 0));
		fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

		fypp->stream_start_produced = true;
		return 0;
	}

	/* XXX scan to next token? */

	c = fy_reader_peek(fyr);

	if (fy_is_z(c)) {

		if (c >= 0)
			fy_reader_advance(fyr, c);

		/* produce stream end continuously */
		fyt = fy_path_token_queue(fypp, FYTT_STREAM_END, fy_reader_fill_atom_a(fyr, 0));
		fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

		return 0;
	}

	fyt = NULL;
	type = FYTT_NONE;
	simple_token_count = 0;

	switch (c) {
	case '/':
		type = FYTT_PE_SLASH;
		simple_token_count = 1;
		break;

	case '^':
		type = FYTT_PE_ROOT;
		simple_token_count = 1;
		break;

	case ':':
		type = FYTT_PE_SIBLING;
		simple_token_count = 1;
		break;

	case '$':
		type = FYTT_PE_SCALAR_FILTER;
		simple_token_count = 1;
		break;

	case '%':
		type = FYTT_PE_COLLECTION_FILTER;
		simple_token_count = 1;
		break;

	case '[':
		if (fy_reader_peek_at(fyr, 1) == ']') {
			type = FYTT_PE_SEQ_FILTER;
			simple_token_count = 2;
		}
		break;

	case '{':
		if (fy_reader_peek_at(fyr, 1) == '}') {
			type = FYTT_PE_MAP_FILTER;
			simple_token_count = 2;
		}
		break;

	case ',':
		type = FYTT_PE_COMMA;
		simple_token_count = 1;
		break;

	case '.':
		if (fy_reader_peek_at(fyr, 1) == '.') {
			type = FYTT_PE_PARENT;
			simple_token_count = 2;
		} else {
			type = FYTT_PE_THIS;
			simple_token_count = 1;
		}
		break;

	case '*':
		if (fy_reader_peek_at(fyr, 1) == '*') {
			type = FYTT_PE_EVERY_CHILD_R;
			simple_token_count = 2;
		} else if (!fy_is_first_alpha(fy_reader_peek_at(fyr, 1))) {
			type = FYTT_PE_EVERY_CHILD;
			simple_token_count = 1;
		} else {
			type = FYTT_PE_ALIAS;
			simple_token_count = 2;
			while (fy_is_alnum(fy_reader_peek_at(fyr, simple_token_count)))
				simple_token_count++;
		}
		break;

	default:
		break;
	}

	/* simple tokens */
	if (simple_token_count > 0) {
		fyt = fy_path_token_queue(fypp, type, fy_reader_fill_atom_a(fyr, simple_token_count));
		fyr_error_check(fyr, fyt, err_out, "fy_path_token_queue() failed\n");

		return 0;
	}

	if (fy_is_first_alpha(c))
		return fy_path_fetch_simple_map_key(fypp, c);

	if (fy_is_path_flow_key_start(c))
		return fy_path_fetch_flow_map_key(fypp, c);

	if (fy_is_num(c) || (c == '-' && fy_is_num(fy_reader_peek_at(fyr, 1))))
		return fy_path_fetch_seq_index_or_slice(fypp, c);

	FYR_PARSE_ERROR(fyr, 0, 1, FYEM_SCAN, "bad path expression starts here");

err_out:
	fypp->stream_error = true;
	rc = -1;
	return rc;
}

struct fy_token *fy_path_scan_peek(struct fy_path_parser *fypp, struct fy_token *fyt_prev)
{
	struct fy_token *fyt;
	struct fy_reader *fyr;
	int rc, last_token_activity_counter;

	fyr = &fypp->reader;

	/* nothing if stream end produced (and no stream end token in queue) */
	if (!fyt_prev && fypp->stream_end_produced && fy_token_list_empty(&fypp->queued_tokens)) {

		fyt = fy_token_list_head(&fypp->queued_tokens);
		if (fyt && fyt->type == FYTT_STREAM_END)
			return fyt;

		return NULL;
	}

	/* we loop until we have a token and the simple key list is empty */
	for (;;) {
		if (!fyt_prev)
			fyt = fy_token_list_head(&fypp->queued_tokens);
		else
			fyt = fy_token_next(&fypp->queued_tokens, fyt_prev);
		if (fyt)
			break;

		/* on stream error we're done */
		if (fypp->stream_error)
			return NULL;

		/* keep track of token activity, if it didn't change
		* after the fetch tokens call, the state machine is stuck
		*/
		last_token_activity_counter = fypp->token_activity_counter;

		/* fetch more then */
		rc = fy_path_fetch_tokens(fypp);
		if (rc) {
			fy_error(fypp->diag, "fy_path_fetch_tokens() failed\n");
			goto err_out;
		}
		if (last_token_activity_counter == fypp->token_activity_counter) {
			fy_error(fypp->diag, "out of tokens and failed to produce anymore");
			goto err_out;
		}
	}

	switch (fyt->type) {
	case FYTT_STREAM_START:
		fypp->stream_start_produced = true;
		break;
	case FYTT_STREAM_END:
		fypp->stream_end_produced = true;

		rc = fy_reader_input_done(fyr);
		if (rc) {
			fy_error(fypp->diag, "fy_parse_input_done() failed");
			goto err_out;
		}
		break;
	default:
		break;
	}

	return fyt;

err_out:
	return NULL;
}

struct fy_token *fy_path_scan_remove(struct fy_path_parser *fypp, struct fy_token *fyt)
{
	if (!fypp || !fyt)
		return NULL;

	fy_token_list_del(&fypp->queued_tokens, fyt);

	return fyt;
}

struct fy_token *fy_path_scan_remove_peek(struct fy_path_parser *fypp, struct fy_token *fyt)
{
	fy_token_unref(fy_path_scan_remove(fypp, fyt));

	return fy_path_scan_peek(fypp, NULL);
}

struct fy_token *fy_path_scan(struct fy_path_parser *fypp)
{
	return fy_path_scan_remove(fypp, fy_path_scan_peek(fypp, NULL));
}

void fy_path_expr_dump(struct fy_path_parser *fypp, struct fy_path_expr *expr, int level, const char *banner)
{
	struct fy_path_expr *expr2;
	const char *text;
	size_t len;

	if (banner)
		fy_notice(fypp->diag, "%-*s%s", level*2, "", banner);

	text = fy_token_get_text(expr->fyt, &len);

	fy_notice(fypp->diag, "> %-*s%s%s%.*s",
			level*2, "",
			path_expr_type_txt[expr->type],
			len ? " " : "",
			(int)len, text);

	for (expr2 = fy_path_expr_list_head(&expr->children); expr2; expr2 = fy_path_expr_next(&expr->children, expr2))
		fy_path_expr_dump(fypp, expr2, level+1, NULL);
}

bool fy_token_type_is_component_start(enum fy_token_type type)
{
	return type == FYTT_PE_ROOT ||
	       type == FYTT_PE_THIS ||
	       type == FYTT_PE_PARENT ||
	       type == FYTT_PE_MAP_KEY ||
	       type == FYTT_PE_SEQ_INDEX ||
	       type == FYTT_PE_SEQ_SLICE ||
	       type == FYTT_PE_EVERY_CHILD ||
	       type == FYTT_PE_EVERY_CHILD_R ||
	       type == FYTT_PE_ALIAS;
}

bool fy_token_type_is_filter(enum fy_token_type type)
{
	return type == FYTT_PE_SCALAR_FILTER ||
	       type == FYTT_PE_COLLECTION_FILTER ||
	       type == FYTT_PE_SEQ_FILTER ||
	       type == FYTT_PE_MAP_FILTER;
}

enum fy_path_expr_type fy_map_token_to_path_expr_type(enum fy_token_type type)
{
	switch (type) {
	case FYTT_PE_ROOT:
		return fpet_root;
	case FYTT_PE_THIS:
		return fpet_this;
	case FYTT_PE_PARENT:
		return fpet_parent;
	case FYTT_PE_MAP_KEY:
		return fpet_map_key;
	case FYTT_PE_SEQ_INDEX:
		return fpet_seq_index;
	case FYTT_PE_SEQ_SLICE:
		return fpet_seq_slice;
	case FYTT_PE_EVERY_CHILD:
		return fpet_every_child;
	case FYTT_PE_EVERY_CHILD_R:
		return fpet_every_child_r;
	case FYTT_PE_ALIAS:
		return fpet_alias;
	case FYTT_PE_SCALAR_FILTER:
		return fpet_assert_scalar;
	case FYTT_PE_COLLECTION_FILTER:
		return fpet_assert_collection;
	case FYTT_PE_SEQ_FILTER:
		return fpet_assert_sequence;
	case FYTT_PE_MAP_FILTER:
		return fpet_assert_mapping;
	default:
		break;
	}
	return fpet_none;
}

bool fy_token_type_is_operand(enum fy_token_type type)
{
	return type == FYTT_PE_ROOT ||
	       type == FYTT_PE_THIS ||
	       type == FYTT_PE_PARENT ||
	       type == FYTT_PE_MAP_KEY ||
	       type == FYTT_PE_SEQ_INDEX ||
	       type == FYTT_PE_SEQ_SLICE ||
	       type == FYTT_PE_EVERY_CHILD ||
	       type == FYTT_PE_EVERY_CHILD_R ||
	       type == FYTT_PE_ALIAS;
}

bool fy_token_type_is_operator(enum fy_token_type type)
{
	return type == FYTT_PE_SLASH ||
	       type == FYTT_PE_SCALAR_FILTER ||
	       type == FYTT_PE_COLLECTION_FILTER ||
	       type == FYTT_PE_SEQ_FILTER ||
	       type == FYTT_PE_MAP_FILTER ||
	       type == FYTT_PE_SIBLING ||
	       type == FYTT_PE_COMMA;
}

bool fy_token_type_is_operand_or_operator(enum fy_token_type type)
{
	return fy_token_type_is_operand(type) ||
	       fy_token_type_is_operator(type);
}

static int
push_operand(struct fy_path_parser *fypp, struct fy_path_expr *expr)
{
	struct fy_path_expr **exprs;
	unsigned int alloc;
	size_t size;

	/* grow the stack if required */
	if (fypp->operand_top >= fypp->operand_alloc) {
		alloc = fypp->operand_alloc;
		size = alloc * sizeof(*exprs);

		if (fypp->operands == fypp->operands_static) {
			exprs = malloc(size * 2);
			if (exprs)
				memcpy(exprs, fypp->operands_static, size);
		} else
			exprs = realloc(fypp->operands, size * 2);

		if (!exprs)
			return -1;

		fypp->operand_alloc = alloc * 2;
		fypp->operands = exprs;
	}

	fypp->operands[fypp->operand_top++] = expr;
	return 0;
}

static struct fy_path_expr *
pop_operand(struct fy_path_parser *fypp)
{
	struct fy_path_expr *expr;

	if (fypp->operand_top == 0)
		return NULL;

	expr = fypp->operands[--fypp->operand_top];
	fypp->operands[fypp->operand_top] = NULL;

	return expr;
}

static int
push_operator(struct fy_path_parser *fypp, struct fy_token *fyt)
{
	struct fy_token **fyts;
	unsigned int alloc;
	size_t size;

	assert(fy_token_type_is_operator(fyt->type));

	/* grow the stack if required */
	if (fypp->operator_top >= fypp->operator_alloc) {
		alloc = fypp->operator_alloc;
		size = alloc * sizeof(*fyts);

		if (fypp->operators == fypp->operators_static) {
			fyts = malloc(size * 2);
			if (fyts)
				memcpy(fyts, fypp->operators_static, size);
		} else
			fyts = realloc(fypp->operators, size * 2);

		if (!fyts)
			return -1;

		fypp->operator_alloc = alloc * 2;
		fypp->operators = fyts;
	}

	fypp->operators[fypp->operator_top++] = fyt;

	return 0;
}

static struct fy_token *
peek_operator(struct fy_path_parser *fypp)
{
	if (fypp->operator_top == 0)
		return NULL;
	return fypp->operators[fypp->operator_top - 1];
}

static struct fy_token *
pop_operator(struct fy_path_parser *fypp)
{
	struct fy_token *fyt;

	if (fypp->operator_top == 0)
		return NULL;

	fyt = fypp->operators[--fypp->operator_top];
	fypp->operators[fypp->operator_top] = NULL;

	return fyt;
}

int fy_token_type_operator_prec(enum fy_token_type type)
{
	switch (type) {
	case FYTT_PE_SLASH:
		return 10;
	case FYTT_PE_SCALAR_FILTER:
	case FYTT_PE_COLLECTION_FILTER:
	case FYTT_PE_SEQ_FILTER:
	case FYTT_PE_MAP_FILTER:
		return 5;
	case FYTT_PE_SIBLING:
		return 20;
	case FYTT_PE_COMMA:
		return 15;
	default:
		break;
	}
	return -1;
}

#define PREFIX	0
#define INFIX	1
#define SUFFIX	2

int fy_token_type_operator_placement(enum fy_token_type type)
{
	switch (type) {
	case FYTT_PE_SLASH:	/* SLASH is special at the start of the expression */
	case FYTT_PE_COMMA:
		return INFIX;
	case FYTT_PE_SCALAR_FILTER:
	case FYTT_PE_COLLECTION_FILTER:
	case FYTT_PE_SEQ_FILTER:
	case FYTT_PE_MAP_FILTER:
		return SUFFIX;
	case FYTT_PE_SIBLING:
		return PREFIX;
	default:
		break;
	}
	return -1;
}

const struct fy_mark *fy_path_expr_start_mark(struct fy_path_expr *expr)
{
	struct fy_path_expr *exprn;

	if (!expr)
		return NULL;

	if (expr->type != fpet_chain && expr->type != fpet_multi)
		return fy_token_start_mark(expr->fyt);

	exprn = fy_path_expr_list_head(&expr->children);
	if (!exprn)
		return NULL;

	return fy_path_expr_start_mark(exprn);
}

const struct fy_mark *fy_path_expr_end_mark(struct fy_path_expr *expr)
{
	struct fy_path_expr *exprn;

	if (!expr)
		return NULL;

	if (expr->type != fpet_chain && expr->type != fpet_multi)
		return fy_token_end_mark(expr->fyt);

	exprn = fy_path_expr_list_tail(&expr->children);
	if (!exprn)
		return NULL;

	return fy_path_expr_end_mark(exprn);
}

static int evaluate(struct fy_path_parser *fypp)
{
	struct fy_reader *fyr;
	struct fy_token *fyt_top = NULL;
	struct fy_path_expr *exprl = NULL, *exprr = NULL, *chain = NULL, *expr = NULL, *multi = NULL;
	const struct fy_mark *m1, *m2;
	int ret;

	fyr = &fypp->reader;

	fyt_top = pop_operator(fypp);
	fyr_error_check(fyr, fyt_top, err_out,
			"pop_operator() failed to find token operator to evaluate\n");

	exprl = NULL;
	exprr = NULL;
	switch (fyt_top->type) {

	case FYTT_PE_SLASH:

		exprr = pop_operand(fypp);
		if (!exprr) {
			/* slash at the beginning is root */

			exprr = fy_path_expr_alloc_recycle(fypp);
			fyr_error_check(fyr, exprr, err_out,
					"fy_path_expr_alloc_recycle() failed\n");

			exprr->type = fpet_root;
			exprr->fyt = fyt_top;
			fyt_top = NULL;

			ret = push_operand(fypp, exprr);
			fyr_error_check(fyr, !ret, err_out,
					"push_operand() failed\n");
			exprr = NULL;
			break;
		}

		exprl = pop_operand(fypp);
		if (!exprl) {

			m1 = fy_token_start_mark(fyt_top);
			m2 = fy_path_expr_start_mark(exprr);

			assert(m1);
			assert(m2);

			/* /foo -> add root */
			if (m1->input_pos < m2->input_pos) {
				/* / is to the left, it's a root */
				exprl = fy_path_expr_alloc_recycle(fypp);
				fyr_error_check(fyr, exprl, err_out,
						"fy_path_expr_alloc_recycle() failed\n");

				exprl->type = fpet_root;
				exprl->fyt = fyt_top;
				fyt_top = NULL;

			} else {

				/* / is to the right, it's a collection marker */

				/* switch exprl with exprr */
				exprl = exprr;
				exprr = NULL;
			}
		}

		/* optimize chains */
		if (exprl->type != fpet_chain) {
			/* chaining */
			chain = fy_path_expr_alloc_recycle(fypp);
			fyr_error_check(fyr, chain, err_out,
					"fy_path_expr_alloc_recycle() failed\n");

			chain->type = fpet_chain;
			chain->fyt = NULL;

			fy_path_expr_list_add_tail(&chain->children, exprl);
		} else {
			/* reuse lhs chain */
			chain = exprl;
			exprl = NULL;
		}

		if (!exprr) {
			/* should never happen, but check */
			assert(fyt_top);

			/* this is a collection marker */
			exprr = fy_path_expr_alloc_recycle(fypp);
			fyr_error_check(fyr, exprr, err_out,
					"fy_path_expr_alloc_recycle() failed\n");

			exprr->type = fpet_assert_collection;
			exprr->fyt = fyt_top;
			fyt_top = NULL;
		}

		if (exprr->type != fpet_chain) {
			/* not a chain, append */
			fy_path_expr_list_add_tail(&chain->children, exprr);
		} else {
			/* move the contents of the chain */
			while ((expr = fy_path_expr_list_pop(&exprr->children)) != NULL)
				fy_path_expr_list_add_tail(&chain->children, expr);
			fy_path_expr_free_recycle(fypp, exprr);
		}

		ret = push_operand(fypp, chain);
		fyr_error_check(fyr, !ret, err_out,
				"push_operand() failed\n");
		chain = NULL;

		fy_token_unref(fyt_top);
		fyt_top = NULL;

		break;

	case FYTT_PE_SIBLING:

		exprr = pop_operand(fypp);

		FYR_TOKEN_ERROR_CHECK(fyr, fyt_top, FYEM_PARSE,
				exprr, err_out,
				"sibling operator without argument");

		FYR_TOKEN_ERROR_CHECK(fyr, fyt_top, FYEM_PARSE,
				exprr->fyt && exprr->fyt->type == FYTT_PE_MAP_KEY, err_out,
				"sibling operator on non-map key");

		/* chaining */
		chain = fy_path_expr_alloc_recycle(fypp);
		fyr_error_check(fyr, chain, err_out,
				"fy_path_expr_alloc_recycle() failed\n");

		chain->type = fpet_chain;
		chain->fyt = fyt_top;
		fyt_top = NULL;

		exprl = fy_path_expr_alloc_recycle(fypp);
		fyr_error_check(fyr, exprl, err_out,
				"fy_path_expr_alloc_recycle() failed\n");

		exprl->type = fpet_parent;
		exprl->fyt = NULL;

		fy_path_expr_list_add_tail(&chain->children, exprl);
		fy_path_expr_list_add_tail(&chain->children, exprr);

		ret = push_operand(fypp, chain);
		fyr_error_check(fyr, !ret, err_out,
				"push_operand() failed\n");
		chain = NULL;

		break;

	case FYTT_PE_COMMA:

		exprr = pop_operand(fypp);
		FYR_TOKEN_ERROR_CHECK(fyr, fyt_top, FYEM_PARSE,
				exprr, err_out,
				"comma without operands (rhs)");

		exprl = pop_operand(fypp);
		FYR_TOKEN_ERROR_CHECK(fyr, fyt_top, FYEM_PARSE,
				exprl, err_out,
				"comma without operands (lhs)");

		/* optimize multi */
		if (exprl->type != fpet_multi) {

			/* multi */
			multi = fy_path_expr_alloc_recycle(fypp);
			fyr_error_check(fyr, multi, err_out,
					"fy_path_expr_alloc_recycle() failed\n");

			multi->type = fpet_multi;
			multi->fyt = fyt_top;
			fyt_top = NULL;

			fy_path_expr_list_add_tail(&multi->children, exprl);
		} else {
			/* reuse lhs chain */
			multi = exprl;
		}

		if (exprr->type != fpet_multi) {
			/* not a chain, append */
			fy_path_expr_list_add_tail(&multi->children, exprr);
		} else {
			/* move the contents of the chain */
			while ((expr = fy_path_expr_list_pop(&exprr->children)) != NULL)
				fy_path_expr_list_add_tail(&multi->children, expr);
			fy_path_expr_free_recycle(fypp, exprr);
		}

		ret = push_operand(fypp, multi);
		fyr_error_check(fyr, !ret, err_out,
				"push_operand() failed\n");
		multi = NULL;

		fy_token_unref(fyt_top);
		fyt_top = NULL;

		break;

	case FYTT_PE_SCALAR_FILTER:
	case FYTT_PE_COLLECTION_FILTER:
	case FYTT_PE_SEQ_FILTER:
	case FYTT_PE_MAP_FILTER:

		exprl = pop_operand(fypp);
		FYR_TOKEN_ERROR_CHECK(fyr, fyt_top, FYEM_PARSE,
				exprl, err_out,
				"filter operator without argument");

		if (exprl->type != fpet_chain) {
			/* chaining */
			chain = fy_path_expr_alloc_recycle(fypp);
			fyr_error_check(fyr, chain, err_out,
					"fy_path_expr_alloc_recycle() failed\n");

			chain->type = fpet_chain;
			chain->fyt = NULL;
		} else
			chain = exprl;

		exprr = fy_path_expr_alloc_recycle(fypp);
		fyr_error_check(fyr, exprr, err_out,
				"fy_path_expr_alloc_recycle() failed\n");

		exprr->type = fy_map_token_to_path_expr_type(fyt_top->type);
		exprr->fyt = fyt_top;
		fyt_top = NULL;

		fy_path_expr_list_add_tail(&chain->children, exprr);

		ret = push_operand(fypp, chain);
		fyr_error_check(fyr, !ret, err_out,
				"push_operand() failed\n");
		chain = NULL;

		break;

	default:
		assert(0);
		break;
	}

	return 0;

err_out:
	fy_token_unref(fyt_top);
	fy_path_expr_free(exprl);
	fy_path_expr_free(exprr);
	fy_path_expr_free(chain);
	fy_path_expr_free(multi);
	return -1;
}

struct fy_path_expr *
fy_path_parse_expression(struct fy_path_parser *fypp)
{
	struct fy_reader *fyr;
	struct fy_token *fyt = NULL, *fyt_top = NULL;
	struct fy_path_expr *expr;
	int ret;

	/* the parser must be in the correct state */
	if (!fypp || fypp->operator_top || fypp->operand_top)
		return NULL;

	fyr = &fypp->reader;

	/* find stream start */
	fyt = fy_path_scan_peek(fypp, NULL);
	FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_PARSE,
			fyt && fyt->type == FYTT_STREAM_START, err_out,
			"no tokens available or start without stream start");

	/* remove stream start */
	fy_token_unref(fy_path_scan_remove(fypp, fyt));
	fyt = NULL;

	while ((fyt = fy_path_scan_peek(fypp, NULL)) != NULL) {

		if (fyt->type == FYTT_STREAM_END)
			break;

		/* if it's an operand convert it to expression and push */
		if (fy_token_type_is_operand(fyt->type)) {

			expr = fy_path_expr_alloc_recycle(fypp);
			fyr_error_check(fyr, expr, err_out,
					"fy_path_expr_alloc_recycle() failed\n");

			expr->fyt = fy_path_scan_remove(fypp, fyt);
			expr->type = fy_map_token_to_path_expr_type(fyt->type);
			fyt = NULL;

			ret = push_operand(fypp, expr);
			fyr_error_check(fyr, !ret, err_out, "push_operand() failed\n");
			expr = NULL;

			continue;
		}

		/* it's an operator */
		for (;;) {
			/* get the top of the operator stack */
			fyt_top = peek_operator(fypp);
			/* if operator stack is empty or the priority of the new operator is larger, push operator */
			if (!fyt_top || fy_token_type_operator_prec(fyt->type) > fy_token_type_operator_prec(fyt_top->type)) {
				fyt = fy_path_scan_remove(fypp, fyt);
				ret = push_operator(fypp, fyt);
				fyr_error_check(fyr, !ret, err_out, "push_operator() failed\n");
				fyt = NULL;
				break;
			}

			ret = evaluate(fypp);
			/* evaluate will print diagnostic on error */
			if (ret)
				goto err_out;
		}
	}

	FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_PARSE,
			fyt && fyt->type == FYTT_STREAM_END, err_out,
			"stream ended without STREAM_END");

	while ((fyt_top = peek_operator(fypp)) != NULL) {
		ret = evaluate(fypp);
		/* evaluate will print diagnostic on error */
		if (ret)
			goto err_out;
	}

	FYR_TOKEN_ERROR_CHECK(fyr, fyt, FYEM_PARSE,
			fypp->operand_top == 1, err_out,
			"invalid operand stack at end");

	/* remove stream end */
	fy_token_unref(fy_path_scan_remove(fypp, fyt));
	fyt = NULL;

	/* and return the last operand */
	return pop_operand(fypp);

err_out:
	fy_token_unref(fyt);
	fypp->stream_error = true;
	return NULL;
}
