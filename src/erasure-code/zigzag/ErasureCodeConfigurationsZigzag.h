// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Matan Liram <matanl@protonmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_CONFIGURATIONS_ZIGZAG_H
#define CEPH_ERASURE_CODE_CONFIGURATIONS_ZIGZAG_H

#define MAX_DATA_CHUNKS     24
#define MAX_PARITY_CHUNKS   5
#define MAX_VIRTUAL_CHUNKS  3
#define MAX_DUPLICATION     10
#define MAX_SIGMA           2
#define MAX_MATRIX          256
#define MAX_ZIGZAG_CHUNKS   4

typedef class Zigzag {
public:
    /* Used for Decoding */
    unsigned int zz_perms[MAX_ZIGZAG_CHUNKS][MAX_MATRIX][MAX_MATRIX];

    /* Used for Encoding */
    unsigned int zz_encode[MAX_ZIGZAG_CHUNKS][MAX_MATRIX][MAX_MATRIX];

    unsigned int zz_coeff[MAX_ZIGZAG_CHUNKS][MAX_MATRIX][MAX_MATRIX];

    /* In case chunk i failed, recover rows parity_rows[i] from the row parity */
    unsigned int parity_rows[MAX_MATRIX][MAX_MATRIX];

    /* In case chunk i failed, recover rows zigzag_rows[i] from the zigzag */
    unsigned int zigzag_rows[MAX_ZIGZAG_CHUNKS][MAX_MATRIX][MAX_MATRIX];

} *PZigzag;

extern const unsigned int zz_transposes[];
extern Zigzag zz_8_2_0_2_1;
extern Zigzag zz_8_2_0_2_0;
extern Zigzag zz_8_2_0_1_0;
extern Zigzag zz_6_2_0_3_0;
extern Zigzag zz_6_2_0_2_0;
extern Zigzag zz_6_2_0_1_0;
extern Zigzag zz_4_2_0_2_0;
extern Zigzag zz_4_2_0_1_0;
extern Zigzag zz_4_2_0_1_1;
extern Zigzag zz_10_3_0_5_0;
extern Zigzag zz_10_3_2_4_1;
extern Zigzag zz_10_3_2_4_0;
extern Zigzag zz_10_3_2_3_0;
extern Zigzag zz_10_3_0_2_0;
extern Zigzag zz_8_3_0_4_0;
extern Zigzag zz_8_3_1_3_1;
extern Zigzag zz_8_3_1_3_0;
extern Zigzag zz_8_3_0_2_0;
extern Zigzag zz_6_3_0_3_0;
extern Zigzag zz_6_3_0_2_1;
extern Zigzag zz_6_3_0_2_0;
extern Zigzag zz_6_3_0_1_0;
extern Zigzag zz_4_3_0_1_0;
extern Zigzag zz_10_4_0_5_0;
extern Zigzag zz_10_4_2_4_1;
extern Zigzag zz_10_4_2_4_0;
extern Zigzag zz_10_4_2_3_0;
extern Zigzag zz_8_4_0_4_0;
extern Zigzag zz_8_4_1_3_1;
extern Zigzag zz_8_4_1_3_0;
extern Zigzag zz_8_4_0_2_0;
extern Zigzag zz_4_4_0_1_0;

class ZigzagConfigs
{
public:
    PZigzag configs[MAX_DATA_CHUNKS][MAX_PARITY_CHUNKS]\
        [MAX_VIRTUAL_CHUNKS][MAX_DUPLICATION][MAX_SIGMA];
    ZigzagConfigs();
};

#endif

