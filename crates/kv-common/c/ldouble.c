/*
 * INCRBYFLOAT arithmetic in C `long double` (DESIGN §8.1, §8.3, §24.3).
 *
 * Parsing and formatting reproduce Redis 7.0 `string2ld()` and
 * `ld2string(..., LD_STR_HUMAN)` from src/util.c, including the input length
 * limit, the strtold acceptance rules (hex floats and values beyond the double
 * range are accepted; NaN is rejected; overflow/underflow to zero is rejected),
 * the "%.17Lf" rendering with trailing zeros and a trailing '.' removed, and
 * the "-0" -> "0" rewrite. Also Redis string2d() (strict strtod) for commands
 * that take a double.
 *
 * Ported from Redis 7.0 (BSD-3-Clause),
 * Copyright (c) 2009-2012, Salvatore Sanfilippo. See NOTICE.
 */
#define _DEFAULT_SOURCE 1

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Redis MAX_LONG_DOUBLE_CHARS: inputs of this length or more are rejected. */
#define KV_MAX_LONG_DOUBLE_CHARS (5 * 1024)

/* Result codes, mirrored in src/ldouble.rs. */
#define KV_LD_OK 0
#define KV_LD_NOT_FLOAT 1
#define KV_LD_NAN_OR_INF 2

/* Returns 1 and stores the parsed value in *dp when s[0..slen) strictly
 * represents a long double (Redis string2ld), else 0. */
static int kv_string2ld(const char *s, size_t slen, long double *dp) {
    char buf[KV_MAX_LONG_DOUBLE_CHARS];
    long double value;
    char *eptr;

    if (slen == 0 || slen >= sizeof(buf)) return 0;
    memcpy(buf, s, slen);
    buf[slen] = '\0';

    errno = 0;
    value = strtold(buf, &eptr);
    if (isspace((unsigned char)buf[0]) || eptr[0] != '\0' ||
        ((errno == ERANGE) &&
         (value == HUGE_VAL || value == -HUGE_VAL || fpclassify(value) == FP_ZERO)) ||
        errno == EINVAL || isnan(value))
        return 0;

    *dp = value;
    return 1;
}

/* Redis ld2string(buf, len, value, LD_STR_HUMAN) for a finite value. Returns
 * the length written (without the terminating NUL), or 0 with buf[0] = '\0'
 * when the rendering does not fit. */
static size_t kv_ld2string_human(char *buf, size_t len, long double value) {
    int n = snprintf(buf, len, "%.17Lf", value);
    size_t l;

    if (n < 0 || (size_t)n + 1 > len) {
        buf[0] = '\0';
        return 0;
    }
    l = (size_t)n;
    /* Remove trailing zeros after the '.', then a trailing '.'. */
    if (strchr(buf, '.') != NULL) {
        char *p = buf + l - 1;
        while (*p == '0') {
            p--;
            l--;
        }
        if (*p == '.') l--;
    }
    if (l == 2 && buf[0] == '-' && buf[1] == '0') {
        buf[0] = '0';
        l = 1;
    }
    buf[l] = '\0';
    return l;
}

/* Redis string2d(): 1 and *dp set when s[0..slen) strictly represents a
 * double (strtod, no leading space, every byte consumed, no overflow, no
 * underflow to zero, not NaN), else 0. */
int kv_string2d(const unsigned char *s, size_t slen, double *dp) {
    char stackbuf[KV_MAX_LONG_DOUBLE_CHARS];
    char *buf = stackbuf;
    double value;
    char *eptr;
    int ok;

    *dp = 0;
    if (slen == 0) return 0;
    /* Redis parses the NUL-terminated argument in place, whatever its length. */
    if (slen >= sizeof(stackbuf)) {
        buf = malloc(slen + 1);
        if (buf == NULL) return 0;
    }
    memcpy(buf, s, slen);
    buf[slen] = '\0';

    errno = 0;
    value = strtod(buf, &eptr);
    ok = !(isspace((unsigned char)buf[0]) || (size_t)(eptr - buf) != slen ||
           (errno == ERANGE &&
            (value == HUGE_VAL || value == -HUGE_VAL || fpclassify(value) == FP_ZERO)) ||
           isnan(value));
    if (buf != stackbuf) free(buf);
    if (ok) *dp = value;
    return ok;
}

/* current (when has_cur) + incr, rendered like Redis INCRBYFLOAT into
 * out[0..out_cap). On KV_LD_OK *out_len holds the result length. */
int kv_incrbyfloat(const unsigned char *cur, size_t cur_len, int has_cur,
                   const unsigned char *incr, size_t incr_len,
                   unsigned char *out, size_t out_cap, size_t *out_len) {
    char buf[KV_MAX_LONG_DOUBLE_CHARS];
    long double value = 0;
    long double by = 0;
    size_t l;

    *out_len = 0;
    if (has_cur && !kv_string2ld((const char *)cur, cur_len, &value)) return KV_LD_NOT_FLOAT;
    if (!kv_string2ld((const char *)incr, incr_len, &by)) return KV_LD_NOT_FLOAT;

    value += by;
    if (isnan(value) || isinf(value)) return KV_LD_NAN_OR_INF;

    l = kv_ld2string_human(buf, sizeof(buf), value);
    if (l > out_cap) l = 0; /* cannot happen: out_cap == KV_MAX_LONG_DOUBLE_CHARS */
    memcpy(out, buf, l);
    *out_len = l;
    return KV_LD_OK;
}
