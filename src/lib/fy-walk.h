/*
 * fy-walk.h - walker  internal header file
 *
 * Copyright (c) 2021 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_WALK_H
#define FY_WALK_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-ctype.h"
#include "fy-utf8.h"
#include "fy-list.h"
#include "fy-typelist.h"
#include "fy-types.h"
#include "fy-diag.h"
#include "fy-dump.h"
#include "fy-docstate.h"
#include "fy-accel.h"
#include "fy-doc.h"

struct fy_walk_result {
	struct list_head node;
	struct fy_node *fyn;
};
FY_TYPE_FWD_DECL_LIST(walk_result);
FY_TYPE_DECL_LIST(walk_result);

enum fy_walk_component_type {
	/* none is analyzed and the others are found */
	fwct_none,
	/* start */
	fwct_start_root,
	fwct_start_alias,
	/* ypath */
	fwct_root,		/* /^ or / at the beginning of the expr */
	fwct_this,		/* /. */
	fwct_parent,		/* /.. */
	fwct_every_child,	// /* every immediate child
	fwct_every_child_r,	// /** every recursive child
	fwct_every_leaf,	// /**$ every leaf node
	fwct_assert_collection,	/* match only collection (at the end only) */
	fwct_assert_scalar,	/* match only scalars (leaves) */
	fwct_assert_sequence,	/* match only sequences */
	fwct_assert_mapping,	/* match only sequences */
	fwct_simple_map_key,
	fwct_seq_index,
	fwct_map_key,		/* complex map key (quoted, flow seq or map) */
	fwct_seq_slice,

	fwct_multi,
	fwct_chain,
};

static inline bool fy_walk_component_type_is_valid(enum fy_walk_component_type type)
{
	return type >= fwct_start_root && type <= fwct_chain;
}

static inline bool
fy_walk_component_type_is_initial(enum fy_walk_component_type type)
{
	return type == fwct_start_root ||
	       type == fwct_start_alias;
}

static inline bool
fy_walk_component_type_is_terminating(enum fy_walk_component_type type)
{
	return type == fwct_every_child_r ||
	       type == fwct_every_leaf ||
	       type == fwct_assert_collection ||
	       type == fwct_assert_scalar ||
	       type == fwct_assert_sequence ||
	       type == fwct_assert_mapping;
}

static inline bool
fy_walk_component_type_is_multi(enum fy_walk_component_type type)
{
	return type == fwct_every_child ||
	       type == fwct_every_child_r ||
	       type == fwct_every_leaf ||
	       type == fwct_seq_slice ||
	       type == fwct_multi ||
	       type == fwct_chain;
}

FY_TYPE_FWD_DECL_LIST(walk_component);
struct fy_walk_component {
	struct list_head node;
	struct fy_walk_ctx *wc;
	struct fy_walk_component *parent;
	struct fy_walk_component_list children;
	const char *comp;
	size_t complen;
	enum fy_walk_component_type type;
	bool multi;
	union {
		struct {
			int index;
		} seq_index;
		struct {
			struct fy_document *fyd;	/* for complex key */
		} map_key;
		struct {
			const char *alias;
			size_t aliaslen;
		} alias;
		struct {
			int start_index;
			int end_index;
		} seq_slice;
	};
};
FY_TYPE_DECL_LIST(walk_component);

struct fy_walk_ctx {
	char *path;	/* work area */
	size_t pathlen;
	struct fy_walk_component_list components;
	struct fy_walk_component *root;
};

struct fy_walk_ctx *fy_walk_create(const char *path, size_t len, struct fy_diag *diag);

void fy_walk_destroy(struct fy_walk_ctx *wc);

int fy_walk_perform(struct fy_walk_ctx *wc, struct fy_walk_result_list *results, struct fy_node *fyn);

void fy_walk_result_free(struct fy_walk_result *fwr);
void fy_walk_result_list_free(struct fy_walk_result_list *results);

struct fy_path_parse_cfg {
	int dummy;
};

enum fy_path_parser_state {
	/* none */
	FYPPS_NONE,
	FYPPS_START,
	FYPPS_SEPARATOR,
	FYPPS_EXPRESSION,
	FYPPS_PREFIX,
	FYPPS_SUFFIX,
};

struct fy_path_parser {
	struct fy_path_parse_cfg cfg;
	struct fy_input *fyi;
	struct fy_mark start_mark;
	size_t current_input_pos;	/* from start of input */
	const void *current_ptr;
	int current_c;			/* current utf8 character at current_ptr (-1 if not cached) */
	int current_w;			/* current utf8 character width */
	size_t current_left;		/* currently left characters into the buffer */
	int line;			/* always on input */
	int column;
	struct fy_diag *diag;
};

#endif
