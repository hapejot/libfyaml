/*
 * fy-input.h - YAML input methods
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_INPUT_H
#define FY_INPUT_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include <libfyaml.h>

#include "fy-typelist.h"
#include "fy-ctype.h"

#ifndef NDEBUG
#define __FY_DEBUG_UNUSED__	/* nothing */
#else
#define __FY_DEBUG_UNUSED__	__attribute__((__unused__))
#endif

struct fy_atom;
struct fy_parser;

enum fy_input_type {
	fyit_file,
	fyit_stream,
	fyit_memory,
	fyit_alloc,
	fyit_callback,
};

struct fy_input_cfg {
	enum fy_input_type type;
	void *userdata;
	union {
		struct {
			const char *filename;
		} file;
		struct {
			const char *name;
			FILE *fp;
			size_t chunk;
		} stream;
		struct {
			const void *data;
			size_t size;
		} memory;
		struct {
			void *data;
			size_t size;
		} alloc;
		struct {
		} callback;
	};
};

enum fy_input_state {
	FYIS_NONE,
	FYIS_QUEUED,
	FYIS_PARSE_IN_PROGRESS,
	FYIS_PARSED,
};

FY_TYPE_FWD_DECL_LIST(input);
struct fy_input {
	struct list_head node;
	enum fy_input_state state;
	struct fy_input_cfg cfg;
	char *name;
	void *buffer;		/* when the file can't be mmaped */
	uint64_t generation;
	size_t allocated;
	size_t read;
	size_t chunk;
	FILE *fp;
	int refs;
	bool json_mode : 1;	/* the input is in JSON format */
	union {
		struct {
			int fd;			/* fd for file and stream */
			void *addr;		/* mmaped for files, allocated for streams */
			size_t length;
		} file;
		struct {
		} stream;
	};
};
FY_TYPE_DECL_LIST(input);

static inline bool fy_input_json_mode(const struct fy_input *fyi)
{
	return fyi && fyi->json_mode;
}

static inline bool fy_input_is_lb(const struct fy_input *fyi, int c)
{
	/* '\r', '\n' are always linebreaks */
	if (fy_is_json_lb(c))
		return true;
	if (fyi && fyi->json_mode)
		return false;
	return fy_is_yaml12_lb(c);
}

static inline bool fy_input_is_lbz(const struct fy_input *fyi, int c)
{
	return fy_input_is_lb(fyi, c) || fy_is_z(c);
}

static inline bool fy_input_is_blankz(const struct fy_input *fyi, int c)
{
	return fy_is_ws(c) || fy_input_is_lbz(fyi, c);
}

static inline bool fy_input_is_flow_ws(const struct fy_input *fyi, int c)
{
	/* space is always allowed */
	if (fy_is_space(c))
		return true;
	/* no other space for JSON */
	if (fyi && fyi->json_mode)
		return false;
	/* YAML allows tab for WS */
	return fy_is_tab(c);
}

static inline bool fy_input_is_flow_blankz(const struct fy_input *fyi, int c)
{
	return fy_input_is_flow_ws(fyi, c) || fy_input_is_lbz(fyi, c);
}

static inline const void *fy_input_start(const struct fy_input *fyi)
{
	const void *ptr = NULL;

	switch (fyi->cfg.type) {
	case fyit_file:
		if (fyi->file.addr) {
			ptr = fyi->file.addr;
			break;
		}
		/* fall-through */

	case fyit_stream:
		ptr = fyi->buffer;
		break;

	case fyit_memory:
		ptr = fyi->cfg.memory.data;
		break;

	case fyit_alloc:
		ptr = fyi->cfg.alloc.data;
		break;

	default:
		break;
	}
	assert(ptr);
	return ptr;
}

static inline size_t fy_input_size(const struct fy_input *fyi)
{
	size_t size;

	switch (fyi->cfg.type) {
	case fyit_file:
		if (fyi->file.addr) {
			size = fyi->file.length;
			break;
		}
		/* fall-through */

	case fyit_stream:
		size = fyi->read;
		break;

	case fyit_memory:
		size = fyi->cfg.memory.size;
		break;

	case fyit_alloc:
		size = fyi->cfg.alloc.size;
		break;

	default:
		size = 0;
		break;
	}
	return size;
}

struct fy_input *fy_input_alloc(void);
void fy_input_free(struct fy_input *fyi);
struct fy_input *fy_input_ref(struct fy_input *fyi);
void fy_input_unref(struct fy_input *fyi);

static inline enum fy_input_state fy_input_get_state(struct fy_input *fyi)
{
	return fyi->state;
}

struct fy_input *fy_input_create(const struct fy_input_cfg *fyic);

const char *fy_input_get_filename(struct fy_input *fyi);

struct fy_input *fy_input_from_data(const char *data, size_t size,
				    struct fy_atom *handle, bool simple);
struct fy_input *fy_input_from_malloc_data(char *data, size_t size,
					   struct fy_atom *handle, bool simple);

void fy_input_close(struct fy_input *fyi);

int fy_parse_input_open(struct fy_parser *fyp, struct fy_input *fyi);
int fy_parse_input_done(struct fy_parser *fyp);
const void *fy_parse_input_try_pull(struct fy_parser *fyp, struct fy_input *fyi,
				    size_t pull, size_t *leftp);

struct fy_reader;

struct fy_reader_ops {
	struct fy_diag *(*get_diag)(struct fy_reader *fyr);
	int (*file_open)(struct fy_reader *fyr, const char *filename);
};

struct fy_reader_input_cfg {
	bool disable_mmap_opt;
};

struct fy_reader {
	const struct fy_reader_ops *ops;

	struct fy_reader_input_cfg current_input_cfg;
	struct fy_input *current_input;

	size_t current_pos;		/* from start of stream */
	size_t current_input_pos;	/* from start of input */
	const void *current_ptr;	/* current pointer into the buffer */
	int current_c;			/* current utf8 character at current_ptr (-1 if not cached) */
	int current_w;			/* current utf8 character width */
	size_t current_left;		/* currently left characters into the buffer */

	int line;			/* always on input */
	int column;
	int tabsize;			/* very experimental tab size for indent purposes */
	int nontab_column;		/* column without accounting for tabs */

	struct fy_diag *diag;
};

void fy_reader_reset(struct fy_reader *fyr);
void fy_reader_init(struct fy_reader *fyr, const struct fy_reader_ops *ops);
void fy_reader_cleanup(struct fy_reader *fyr);

int fy_reader_input_open(struct fy_reader *fyr, struct fy_input *fyi, const struct fy_reader_input_cfg *icfg);
int fy_reader_input_done(struct fy_reader *fyr);

const void *fy_reader_ptr_slow_path(struct fy_reader *fyr, size_t *leftp);
const void *fy_reader_ensure_lookahead_slow_path(struct fy_reader *fyr, size_t size, size_t *leftp);

static inline void
fy_reader_get_mark(struct fy_reader *fyr, struct fy_mark *fym)
{
	assert(fyr);
	fym->input_pos = fyr->current_input_pos;
	fym->line = fyr->line;
	fym->column = fyr->column;
}

static inline const void *
fy_reader_ptr(struct fy_reader *fyr, size_t *leftp)
{
	if (fyr->current_ptr) {
		if (leftp)
			*leftp = fyr->current_left;
		return fyr->current_ptr;
	}

	return fy_reader_ptr_slow_path(fyr, leftp);
}

static inline bool
fy_reader_is_lb(const struct fy_reader *fyr, int c)
{
	return fyr && fy_input_is_lb(fyr->current_input, c);
}

static inline bool
fy_reader_is_lbz(const struct fy_reader *fyr, int c)
{
	return fyr && fy_input_is_lbz(fyr->current_input, c);
}

static inline bool
fy_reader_is_blankz(const struct fy_reader *fyr, int c)
{
	return fyr && fy_input_is_blankz(fyr->current_input, c);
}

static inline bool
fy_reader_is_flow_ws(const struct fy_reader *fyr, int c)
{
	return fyr && fy_input_is_flow_ws(fyr->current_input, c);
}

static inline bool
fy_reader_is_flow_blank(const struct fy_reader *fyr, int c)
{
	return fy_reader_is_flow_ws(fyr, c);
}

static inline bool
fy_reader_is_flow_blankz(const struct fy_reader *fyr, int c)
{
	return fyr && fy_input_is_flow_blankz(fyr->current_input, c);
}

static inline const void *
fy_reader_ensure_lookahead(struct fy_reader *fyr, size_t size, size_t *leftp)
{
	if (fyr->current_ptr && fyr->current_left >= size) {
		if (leftp)
			*leftp = fyr->current_left;
		return fyr->current_ptr;
	}
	return fy_reader_ensure_lookahead_slow_path(fyr, size, leftp);
}

/* advance the given number of ascii characters, not utf8 */
static inline void
fy_reader_advance_octets(struct fy_reader *fyr, size_t advance)
{
	struct fy_input *fyi;
	size_t left __FY_DEBUG_UNUSED__;

	assert(fyr);
	assert(fyr->current_input);

	assert(fyr->current_left >= advance);

	fyi = fyr->current_input;

	switch (fyi->cfg.type) {
	case fyit_file:
		if (fyi->file.addr) {
			left = fyi->file.length - fyr->current_input_pos;
			break;
		}
		/* fall-through */

	case fyit_stream:
		left = fyi->read - fyr->current_input_pos;
		break;

	case fyit_memory:
		left = fyi->cfg.memory.size - fyr->current_input_pos;
		break;

	case fyit_alloc:
		left = fyi->cfg.alloc.size - fyr->current_input_pos;
		break;

	default:
		assert(0);	/* no streams */
		break;
	}

	assert(left >= advance);

	fyr->current_input_pos += advance;
	fyr->current_ptr += advance;
	fyr->current_left -= advance;
	fyr->current_pos += advance;

	fyr->current_c = fy_utf8_get(fyr->current_ptr, fyr->current_left, &fyr->current_w);
}

/* compare string at the current point (n max) */
static inline int
fy_reader_strncmp(struct fy_reader *fyr, const char *str, size_t n)
{
	const char *p;
	int ret;

	p = fy_reader_ensure_lookahead(fyr, n, NULL);
	if (!p)
		return -1;
	ret = strncmp(p, str, n);
	return ret ? 1 : 0;
}

static inline int
fy_reader_peek_at_offset(struct fy_reader *fyr, size_t offset)
{
	const uint8_t *p;
	size_t left;
	int w;

	if (offset == 0 && fyr->current_w)
		return fyr->current_c;

	/* ensure that the first octet at least is pulled in */
	p = fy_reader_ensure_lookahead(fyr, offset + 1, &left);
	if (!p)
		return FYUG_EOF;

	/* get width by first octet */
	w = fy_utf8_width_by_first_octet(p[offset]);
	if (!w)
		return FYUG_INV;

	/* make sure that there's enough to cover the utf8 width */
	if (offset + w > left) {
		p = fy_reader_ensure_lookahead(fyr, offset + w, &left);
		if (!p)
			return FYUG_PARTIAL;
	}

	return fy_utf8_get(p + offset, left - offset, &w);
}

static inline int
fy_reader_peek_at_internal(struct fy_reader *fyr, int pos, ssize_t *offsetp)
{
	int i, c;
	size_t offset;

	if (!offsetp || *offsetp < 0) {
		for (i = 0, offset = 0; i < pos; i++, offset += fy_utf8_width(c)) {
			c = fy_reader_peek_at_offset(fyr, offset);
			if (c < 0)
				return c;
		}
	} else
		offset = (size_t)*offsetp;

	c = fy_reader_peek_at_offset(fyr, offset);

	if (offsetp)
		*offsetp = offset + fy_utf8_width(c);

	return c;
}

static inline bool
fy_reader_is_blank_at_offset(struct fy_reader *fyr, size_t offset)
{
	return fy_is_blank(fy_reader_peek_at_offset(fyr, offset));
}

static inline bool
fy_reader_is_blankz_at_offset(struct fy_reader *fyr, size_t offset)
{
	return fy_reader_is_blankz(fyr, fy_reader_peek_at_offset(fyr, offset));
}

static inline int
fy_reader_peek_at(struct fy_reader *fyr, int pos)
{
	return fy_reader_peek_at_internal(fyr, pos, NULL);
}

static inline int
fy_reader_peek(struct fy_reader *fyr)
{
	return fy_reader_peek_at_offset(fyr, 0);
}

static inline void
fy_reader_advance(struct fy_reader *fyr, int c)
{
	bool is_line_break = false;

	/* skip this character */
	fy_reader_advance_octets(fyr, fy_utf8_width(c));

	/* first check for CR/LF */
	if (c == '\r' && fy_reader_peek(fyr) == '\n') {
		fy_reader_advance_octets(fyr, 1);
		is_line_break = true;
	} else if (fy_reader_is_lb(fyr, c))
		is_line_break = true;

	if (is_line_break) {
		fyr->column = 0;
		fyr->nontab_column = 0;
		fyr->line++;
	} else if (fyr->tabsize && fy_is_tab(c)) {
		fyr->column += (fyr->tabsize - (fyr->column % fyr->tabsize));
		fyr->nontab_column++;
	} else {
		fyr->column++;
		fyr->nontab_column++;
	}
}

static inline int
fy_reader_get(struct fy_reader *fyr)
{
	int value;

	value = fy_reader_peek(fyr);
	if (value < 0)
		return value;

	fy_reader_advance(fyr, value);

	return value;
}

static inline int
fy_reader_advance_by(struct fy_reader *fyr, int count)
{
	int i, c;

	for (i = 0; i < count; i++) {
		c = fy_reader_get(fyr);
		if (c < 0)
			break;
	}
	return i ? i : -1;
}

/* compare string at the current point */
static inline bool
fy_reader_strcmp(struct fy_reader *fyr, const char *str)
{
	return fy_reader_strncmp(fyr, str, strlen(str));
}

#endif
