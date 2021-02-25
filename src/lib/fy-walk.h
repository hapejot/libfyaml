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
};

struct fy_walk_ctx *fy_walk_create(const char *path, size_t len, struct fy_diag *diag);

void fy_walk_destroy(struct fy_walk_ctx *wc);

int fy_walk_perform(struct fy_walk_ctx *wc, struct fy_walk_result_list *results, struct fy_node *fyn);

void fy_walk_result_free(struct fy_walk_result *fwr);
void fy_walk_result_list_free(struct fy_walk_result_list *results);

/*********************/

enum fy_path_expr_type {
	/* ypath */
	fpet_root,		/* /^ or / at the beginning of the expr */
	fpet_this,		/* /. */
	fpet_parent,		/* /.. */
	fpet_every_child,	// /* every immediate child
	fpet_every_child_r,	// /** every recursive child
	fpet_every_leaf,	// /**$ every leaf node
	fpet_assert_collection,	/* match only collection (at the end only) */
	fpet_assert_scalar,	/* match only scalars (leaves) */
	fpet_assert_sequence,	/* match only sequences */
	fpet_assert_mapping,	/* match only sequences */
	fpet_simple_map_key,
	fpet_seq_index,
	fpet_map_key,		/* complex map key (quoted, flow seq or map) */
	fpet_seq_slice,

	fpet_multi,
	fpet_chain,
};

extern const char *path_expr_type_txt[];

static inline bool fy_path_expr_type_is_valid(enum fy_path_expr_type type)
{
	return type >= fpet_root && type <= fpet_chain;
}

FY_TYPE_FWD_DECL_LIST(path_expr);
struct fy_path_expr {
	struct list_head node;
	struct fy_path_expr *parent;
	struct fy_path_expr_list children;
	struct fy_atom handle;
};
FY_TYPE_DECL_LIST(path_expr);

struct fy_path_parser {
	struct fy_diag *diag;
	struct fy_reader reader;
	struct fy_path_expr *root;
};

void fy_path_parser_setup(struct fy_path_parser *fypp, struct fy_diag *diag);
void fy_path_parser_cleanup(struct fy_path_parser *fypp);

struct fy_path_expr *
fy_path_parser_parse(struct fy_path_parser *fypp, struct fy_path_expr *parent);

int fy_path_expr_eval(struct fy_path_expr *expr, struct fy_walk_result_list *results, struct fy_node *fyn);

#endif
