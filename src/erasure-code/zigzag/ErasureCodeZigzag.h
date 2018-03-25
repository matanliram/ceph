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

#ifndef CEPH_ERASURE_CODE_ZIGZAG_H
#define CEPH_ERASURE_CODE_ZIGZAG_H

#include "include/err.h"
#include "erasure-code/ErasureCode.h"
#include "ErasureCodeConfigurationsZigzag.h"
extern "C" {
#include <gf_complete.h>
}

class ErasureCodeZigzag : public ErasureCode {
private:
  ZigzagConfigs zz_configs;
  PZigzag pZZ_G;
  unsigned int rows;
  bool zero_elements[MAX_MATRIX][MAX_MATRIX];

  int chunk_index(unsigned int i) const;
  void do_parity(int k, char *data_ptrs[], char *parity_ptr, int size);

  unsigned int get_field() const {
      return w;
  }

  void galois_multiply(gf_t *gf,
                       char *region,
                       unsigned long multby,
                       int nbytes,
                       char *r2,
                       int add);

public:
  int k;
  std::string DEFAULT_K;
  int v;
  std::string DEFAULT_V;
  int r;
  std::string DEFAULT_R;
  int s;
  std::string DEFAULT_S;
  int perm;
  std::string DEFAULT_PERM;
  int align;
  std::string DEFAULT_ALIGN;
  int w;
  std::string DEFAULT_W;
  int stripe_unit;
  std::string DEFAULT_STRIPE_UNIT;
  bool per_chunk_alignment;

  explicit ErasureCodeZigzag() :
    pZZ_G(),
    rows(0),
    k(0),
    DEFAULT_K("2"),
    v(0),
    DEFAULT_V("0"),
    r(0),
    DEFAULT_R("1"),
    s(0),
    DEFAULT_S("1"),
    perm(0),
    DEFAULT_PERM("0"),
    align(0),
    DEFAULT_ALIGN("4096"),
    w(0),
    DEFAULT_W("8"),
    stripe_unit(0),
    DEFAULT_STRIPE_UNIT("1048576"),
    per_chunk_alignment(false)
    {}

  virtual ~ErasureCodeZigzag() {}

  unsigned int get_alignment() const;

  int parse(ErasureCodeProfile &profile, ostream *ss);

  int sanity_check_kvs(int k, int v, int s, ostream *ss);

  int init(ErasureCodeProfile &profile, ostream *ss) override;

  set<int> get_erasures(const set<int> &need,
      const set<int> &available) const;

  unsigned int get_chunk_size(unsigned int object_size) const override;

  unsigned int get_chunk_count() const override {
    return k + r;
  }

  unsigned int get_data_chunk_count() const override {
    return k;
  }

  unsigned int get_virtual_chunk_count() const {
      return v;
  }

  unsigned int get_zigzag_duplication() const {
    return s;
  }

  unsigned int get_row_count() const {
    return (unsigned int) std::pow(r, (k+v)/s - 1);
  }

  unsigned int get_stripe_unit() const{
    return stripe_unit;
  }

  // ###########################
  // # Required_to_reconstruct #
  // ###########################

  // legacy
  virtual int minimum_to_decode(const set<int> &want_to_read,
      const set<int> &available,
      set<int> *minimum) override;

  /*
   * Name: required_to_reconstruct
   *
   * Description:
   *
   * Parameters:
   * @chunks_to_read - the set of want_to_read
   * @available_chunks - set of chunks which are in and up
   * @needed_chunks - output set of needed to read chunks (available_chunks in case
   *    of a single failure, chunks_to_read in case of no failures and undefined in
   *    case of 2 or more failures.
   * @elements_map - output map which gives the read lines set for each available disk.
   *
   * Return:
   * 0 - on success
   * -EINVAL in case of more than 2 erasures
   * -EIO in case there are less available chunks than min_avail.
   *
   * Notes:
   *
   */

  virtual int required_to_reconstruct(const set<int> &chunks_to_read,
      const set<int> &available_chunks,
      set<int> *needed_chunks,
      map<int, set<int> > *elements_map);
private:
  /*
   * Name: required_to_reconstruct_aux
   *
   * Description:
   * This function updates elements_map, if it's null we return immediately
   * For each available chunk initialize elements_map with an empty set, and for each
   * such set, fill the read lines in case of the failure of failed_chunk.
   *
   * Parameters:
   * @chunks_to_read - the set of want_to_read
   * @available_chunks - set of chunks which are in and up
   * @failed_chunk - the single failed chunk
   * @elements_map - output map which gives the read lines set for each available disk.
   *
   * Return:
   * 0 - on success
   * Notes:
   *
   */
  virtual int required_to_reconstruct_aux(const set<int> &chunks_to_read,
      const set<int> &available_chunks,
      const unsigned int failed_chunk,
      map<int, set<int> > *elements_map);


  // ##########
  // # encode #
  // ##########

  virtual int init_zigzag();

public:
  virtual int encode_prepare(const bufferlist &raw,
      map<int, bufferlist> &encoded);

  /*
   * Name: encode
   *
   * Description:
   * This function encodes the data according to the set
   * want_to_encode and writes the encoded data into encoded_data.
   *
   * Parameters:
   * want_to_encode - the chunks we want to encode
   * data - the actual data we want to encode
   * encoded - the output of the encoded data
   *
   * Return:
   * 0 - on success
   *
   * Notes:
   *
   */
  virtual int encode(const set<int> &want_to_encode,
      const bufferlist &in,
      map<int, bufferlist> *encoded);

  int encode_chunks(const set<int> &want_to_encode,
      map<int, bufferlist> *encoded) override;

private:

  /*
   * Name: encode_chunks_zigzag
   *
   * Description:
   * This function encodes the the zigzag parity bit
   *
   * Parameters:
   * chunks - an array of chunks
   * chunk_size - the size of a single chunk
   *
   * Return:
   *
   * Notes:
   *
   */
  virtual void encode_chunks_zigzag(char *chunks[],
      unsigned int chunk_size,
      unsigned int zz_chunk);

public:
  // ###############
  // # Reconstruct #
  // ###############

  // legacy
  virtual int decode_chunks(const set<int> &want_to_read,
    const map<int, bufferlist> &chunks,
    map<int, bufferlist> *reconstructed);

  /*
   * Name: reconstruct_concat
   *
   * Description:
   * This function suits reconstructed input to be sorted by data and only then
   * parity chunks. chunks_to_read and writes the encoded data into reconstructed.
   *
   * Parameters:
   * chunks - the actual data we have
   * reconstructed - the output of the reconstructed data
   *
   * Return:
   * 0 - on success
   * -EINVAL - if there are more than 1 failures
   *
   * Notes:
   * Orders reconstructed according to chunk_mapping. Data chunks first and then
   * parity chunks
   */

  virtual int reconstruct_concat(const map<int, bufferlist> &chunks,
      bufferlist *reconstructed);

  /*
   * Name: reconstruct
   *
   * Description:
   * This function encodes the data according to the set
   * chunks_to_read and writes the encoded data into reconstructed.
   *
   * Parameters:
   * chunks_to_read - the chunks we want to read
   * chunks - the actual data we have
   * reconstructed - the output of the reconstructed data
   *
   * Return:
   * 0 - on success
   * -EINVAL - if there are more than 1 failures
   *
   * Notes:
   * reconstructed has initialized empty bufferlists
   * It initializes reconstructed while reconstruct_chunks doesn't and thus
   * can't be called separately
   */
  virtual int reconstruct(const set<int> &chunks_to_read,
      const map<int, bufferlist> &chunks,
      map<int, bufferlist> *reconstructed);

  /*
   * Name: reconstruct_chunks
   *
   * Description:
   * This function reconstructs the failed chunk into reconstructed
   *
   * Parameters:
   * want_to_read - The chunk indexes we would like to read from
   * read_chunks - Chunks that we can read to decode from
   * reconstructed - The map of outputted data
   *
   * Return:
   * 0 on success
   *
   * Notes:
   *
   */
  virtual int reconstruct_chunks(const set<int> &want_to_read,
      const map<int, bufferlist> &chunks,
      map<int, bufferlist> &reconstructed);
private:

  // already assumes 1 failure exactly, has initialized reconstructed array and failed
  // chunk index.
  int reconstruct_chunks_aux(unsigned int failed_chunk,
      const map<int, bufferlist> &chunks,
      map<int, bufferlist> *reconstructed);

  /*
   * Name: reconstruct_chunks_parity
   *
   * Description:
   * This function reconstructs some of the chunks elements with
   * the xor parity
   *
   * Parameters:
   * failed_chunk - the index of the failed chunk
   * chunks - an array of chunks
   * chunk_size - the size of a single chunk
   *
   * Return:
   * 0 on success
   *
   * Notes:
   *
   */
  virtual int reconstruct_chunks_parity(unsigned int failed_chunk,
      const map<int, bufferlist> &read_chunks, // delete?
      char *chunks[],
      unsigned int chunk_size);

  /*
   * Name: reconstruct_chunks_zigzag
   *
   * Description:
   * This function reconstructs some of the chunks elements with
   * the zigzag parity
   *
   * Parameters:
   * failed_chunk - the index of the failed chunk
   * chunks - an array of chunks
   * chunk_size - the size of a single chunk
   *
   * Return:
   * 0 on success
   *
   * Notes:
   *
   */
  virtual int reconstruct_chunks_zigzag(unsigned int failed_chunk,
      const map<int, bufferlist> &read_chunks, // delete?
      char *chunks[],
      unsigned int chunk_size,
      unsigned int zz_chunk);
public:
};

#endif

