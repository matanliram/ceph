// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * ceph - scalable distributed file system
 *
 * copyright (c) 2014 cloudwatt <libre.licensing@cloudwatt.com>
 * copyright (c) 2014 red hat <contact@redhat.com>
 * copyright (c) 2016 loic dachary <loic@dachary.org>
 *
 * Author: Matan Liram <matanl@protonmail.com>
 *
 *  this library is free software; you can redistribute it and/or
 *  modify it under the terms of the gnu lesser general public
 *  license as published by the free software foundation; either
 *  version 2.1 of the license, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <algorithm>

#include "common/debug.h"
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "include/stringify.h"
#include "erasure-code/ErasureCodePlugin.h"
extern "C" {
#include "jerasure.h"
#include "galois.h"
}

#include "ErasureCodeZigzag.h"

// re-include our assert to clobber boost's

#include "ErasureCodeConfigurationsZigzag.h"

#define DEFAULT_RULE_ROOT "default"
#define DEFAULT_RULE_FAILURE_DOMAIN "osd"

#define dout_context g_ceph_context

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeZigzag: ";
}

static ostream &operator<<(ostream &lhs, const map<int, set<int>> &rhs)
{
  lhs << "[";
  for (map<int, set<int>>::const_iterator i = rhs.begin();
      i != rhs.end();
      ++i) {
    if (i != rhs.begin())
      lhs << ", ";
    lhs << i->first << "(" << i->second << ")";
  }
  return lhs << "]";
}

static ostream &operator<<(ostream &lhs, const set<int> &rhs) {
  lhs << "[";
  for (set<int>::const_iterator i = rhs.begin();
      i != rhs.end();
      ++i) {
    if (i != rhs.begin())
      lhs << ", ";
    lhs << *i;
  }
  return lhs << "]";
}

unsigned ErasureCodeZigzag::get_alignment() const
{
  // dout(1) << "get_alignment" << dendl;
  // return k*w*sizeof(int);
  //unsigned element_alignment = align; // TODO: make sure it's correct
  return align * get_row_count();
}

int ErasureCodeZigzag::parse(ErasureCodeProfile &profile,
			  ostream *ss)
{
  int err = ErasureCode::parse(profile, ss);
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  err |= to_int("v", profile, &v, DEFAULT_V, ss);
  err |= to_int("r", profile, &r, DEFAULT_R, ss);
  //dout(1) << "Matan: k = " << k << " and r = " << r << dendl;
  err |= to_int("s", profile, &s, DEFAULT_S, ss);
  err |= to_int("perm", profile, &perm, DEFAULT_PERM, ss);
  err |= to_int("align", profile, &align, DEFAULT_ALIGN, ss);
  err |= to_int("w", profile, &w, DEFAULT_W, ss);
  err |= to_int("stripe_unit", profile, &stripe_unit, DEFAULT_STRIPE_UNIT, ss);

  if (chunk_mapping.size() > 0 && (int)chunk_mapping.size() != k + r) {
    *ss << "mapping " << profile.find("mapping")->second
        << " maps " << chunk_mapping.size() << " chunks instead of"
        << " the expected " << k + r << " and will be ignored" << std::endl;
    chunk_mapping.clear();
    err = -EINVAL;
  }
  err |= sanity_check_k(k, ss);
  err |= sanity_check_kvs(k, v, s, ss);
  rows = (unsigned) std::pow(r, (k+v)/s - 1); 
  return err;
}

int ErasureCodeZigzag::sanity_check_kvs(int k, int v, int s, ostream *ss)
{
  if ((k+v)%s != 0 || k <= 0 || s <= 0 || v < 0) {
    *ss << "k+v=" << (k+v) << " should divide by s=" << s << std::endl;
    return -EINVAL;
  } else {
    return 0;
  }
}

int ErasureCodeZigzag::init(ErasureCodeProfile &profile,
    ostream *ss)
{
  int err = 0;
  err |= to_string("crush-root", profile,
      &rule_root,
      DEFAULT_RULE_ROOT, ss);
  err |= to_string("crush-failure-domain", profile,
      &rule_failure_domain,
      DEFAULT_RULE_FAILURE_DOMAIN, ss);
  err |= to_string("crush-device-class", profile,
		   &rule_device_class,
		   "", ss);
  if (err)
    return err;

  err = parse(profile, ss);
  if (err)
    return err;

  err = init_zigzag();
  if (err)
    return err;

  ErasureCode::init(profile, ss);
  return 0;
}

set<int> ErasureCodeZigzag::get_erasures(const set<int> &want,
    const set<int> &available) const
{
  //dout(1) << "get_erasures" << dendl;
  set<int> result;
  set_difference(want.begin(), want.end(),
      available.begin(), available.end(),
      inserter(result, result.end()));
  return result;
}

int ErasureCodeZigzag::chunk_index(unsigned i) const
{
  //dout(1) << "chunk_index" << dendl;
  return chunk_mapping.size() > i ? chunk_mapping[i] : i;
}

// get chunk alignment which is 4k*num_rows and returns an aligned chunk's size
// Notice that object size is only the data part of the object
unsigned ErasureCodeZigzag::get_chunk_size(unsigned object_size) const
{
  // dout(1) << "get_chunk_size" << dendl;
  // align to num_row * 4k
  unsigned alignment = get_alignment();
  unsigned data_chunk_count = get_data_chunk_count();
  // ceil:
  unsigned chunk_size = (object_size + data_chunk_count - 1) / data_chunk_count;
  unsigned modulo = chunk_size % alignment;
  dout(20) << "get_chunk_size: chunk_size " << chunk_size
    << " must be modulo " << alignment << dendl;
  if (modulo) {
    dout(10) << "get_chunk_size: original chunk: " << chunk_size
      << " padded to: " << chunk_size + alignment - modulo << dendl;
    chunk_size += alignment - modulo;
  }
  return chunk_size;
}

void p(const set<int> &s) { cerr << s; } // for gdb

int ErasureCodeZigzag::minimum_to_decode(const set<int> &want_to_read,
    const set<int> &available_chunks,
    set<int> *minimum)
{
  assert("ErasureCodeZigzag::minimum_to_decode not implemented" == 0); 
}

// ###########################
// # Required_to_reconstruct #
// ###########################

// if elements_map == NULL, just ignore and don't output to it.
// chunks_to_read are after chunk mapping.
int ErasureCodeZigzag::required_to_reconstruct(const set<int> &chunks_to_read,
    const set<int> &available_chunks,
    set<int> *needed_chunks,
    map<int, set<int> > *elements_map)
{
  dout(1) << "required_to_reconstruct" << dendl;
  dout(1) << "elements_map of size " << elements_map->size() << dendl;
  if (!needed_chunks) {
    return -EINVAL;
  }

  // 0 failures
  // TODO: what if coding data is corrupted?
  if (includes(available_chunks.begin(), available_chunks.end(),
        chunks_to_read.begin(), chunks_to_read.end())) {
    dout(1) << "required_to_reconstruct has 0 erasures"
      << dendl;
    *needed_chunks = chunks_to_read;
    // MatanLiramV10: removed since the memory is not well allocated here on stack
    // and since map is never empty when we put it here.
    //if (elements_map) {
    //  *elements_map = map<int, set<int> >();
    //}
  } else { // 1 or more failres

    // MatanLiramV6: In case there's less than n-1 chunks and we have a failure
    // (after the code from above there's at least one failure), we can't
    // recover it.
    unsigned min_avail = get_chunk_count() - 1;
    if (available_chunks.size() < (unsigned) min_avail) {
      dout(1) << "required_to_reconstruct has less than "
        "n-1 chunks and at least 1 erasure, failure." << dendl;
      return -EIO;
    }

    // erasures = chunks_to_read - available_chunks
    set<int> erasures = get_erasures(chunks_to_read, available_chunks);
    // if 0 erasures we messed up in the "includes" line, otherwise we should have returned -EIO above
    assert(erasures.size() == 1);
    // erased chunk must be a non-negative number
    assert(*(erasures.begin()) >= 0);
    const unsigned failed_chunk = (unsigned) *(erasures.begin());

    // update needed chunks
    set<int> result;
    dout(1) << "required_to_reconstruct: gives n-1 needed_chunks, "
      << "missing chunk is: " << failed_chunk << ", available.size(): "
      << available_chunks.size() << dendl;
    // notice that *needed is always available since erasures are non-available
    // chunks by definition. since we need n-1 chunks this is true.
    // Therefore, the following code is equivalent:
    // *needed_chunks = available_chunks
    std::set_difference(available_chunks.begin(), available_chunks.end(),
        erasures.begin(), erasures.end(),
        std::inserter(*needed_chunks, (*needed_chunks).end()));
    // make sure we use all available chunks (n or n-1) without the erased chunk
    assert(needed_chunks->size() == get_chunk_count() - 1);

    // we repair a parity shard
    if (failed_chunk >= get_data_chunk_count()) {
      for (int i = get_data_chunk_count(); (unsigned) i < get_chunk_count(); i++) {
        // we don't need to read coding chunks in that case actually we also
        // don't need elements map, we'll see what to do about that
        needed_chunks->erase(i);
      }
    }

    if (elements_map == NULL) {
      // Not unclear, it happens in predicate recoverable
      //dout(1) << "required_to_reconstruct: unclear flow in which elements_map"
      //   << " is null but we have 1 failure" << dendl;
      // incorrect, we need all chunks to recover
      //*needed_chunks = erasures;
      // MatanLiramV6: Let ceph know that recovery is possible, flow is unclear here
      return 0;
    }

    // fill elements_map if it is not null:
    return required_to_reconstruct_aux(
        chunks_to_read,
        available_chunks,
        failed_chunk,
        elements_map);
  } // ENDOF >= 1 failures
  // in any case of failures if we passed all checks return "Success"
  return 0;

}


// Fills elements_map
int ErasureCodeZigzag::required_to_reconstruct_aux(const set<int> &chunks_to_read,
    const set<int> &available_chunks,
    const unsigned failed_chunk,
    map<int, set<int> > *elements_map)
{
  // dout(1) << "required_to_reconstruct_aux" << dendl;

  // we handled that scenario in the caller
  assert(elements_map != NULL);
  
  // in case we repair a parity shard
  if (failed_chunk >= get_data_chunk_count()) {
    dout(1) << "required_to_reconstruct_aux: repairing a parity shard" << dendl;
    for (set<int>::const_iterator i = available_chunks.begin();
          i != available_chunks.end(); ++i) {
      // MatanLiramV11: < and not <= since we don't want row parity here!
      if ((unsigned) *i < get_data_chunk_count()) {
        for (unsigned row_index = 0; row_index < rows; ++row_index) {
          if (zero_elements[*i][row_index] == false) {
            (*elements_map)[*i].insert(row_index);
          }
        }
      }
    }
    dout(1) << "MatanLiramV7: required_to_reconstruct_aux: elements_map: "
      << *elements_map << dendl;
    return 0;
  }
  // Else, we repair a systematic shard:

  // iterate parity rows:
  unsigned rows = get_row_count();
  unsigned row_section = rows / get_coding_chunk_count();
  unsigned s = get_zigzag_duplication();
  unsigned real_k = get_data_chunk_count() + get_virtual_chunk_count();
  assert(real_k%s == 0);
  unsigned twin_size = real_k/s;

  // iterate all columns in elements_map, each column should add row_section idx
  for (set<int>::const_iterator i = available_chunks.begin();
      i != available_chunks.end(); ++i) {
    // if twin element insert all rows
    if (((unsigned) *i) % twin_size == failed_chunk % twin_size
        && (unsigned) *i < get_data_chunk_count()) {
      for (int j = 0; (unsigned) j < rows; j++) {
        // if not a zero element
        if (zero_elements[*i][j] == false) {
          (*elements_map)[*i].insert(j);
        }
      }
    } else { // otherwise insert parity rows (or zz rows in case of zz)

      // iterate rows in row_section
      for (unsigned index = 0; index < row_section; ++index) {
        // up to row parity including
        if ((unsigned) *i <= get_data_chunk_count()) {
          unsigned parity_row = pZZ_G->parity_rows[failed_chunk][index];
          // if not a zero element
          if (zero_elements[*i][parity_row] == false) {
            (*elements_map)[*i].insert(parity_row);
          }
        }
        else { // one of the zz chunks
          unsigned zz_index = *i - get_data_chunk_count() - 1;
          unsigned zz_row = pZZ_G->zigzag_rows[zz_index][failed_chunk][index];
          // if not a zero element
          if (zero_elements[*i][zz_row] == false) {
            (*elements_map)[*i].insert(zz_row);
          }
        }
      }
    }
  }

  // print elements_map
  dout(1) << "MatanLiramV7: required_to_reconstruct_aux: elements_map: "
    << *elements_map << dendl;
  return 0;
}


// ##########
// # encode #
// ##########

int ErasureCodeZigzag::init_zigzag()//unsigned int object_size)
{
  // won't print anyways..
  //dout(1) << "init_zigzag" << dendl;
  unsigned int data_chunk_count = get_data_chunk_count();

  pZZ_G = zz_configs.configs[k][r][v][s][perm];

  if (pZZ_G == NULL) {
      return -EINVAL;
  }

  unsigned objsize = get_stripe_unit() * get_data_chunk_count();
  unsigned int blocksize = get_chunk_size(objsize);
  // say objsize is composed of q*blocksize + r, padded_chunks will give k-q
  unsigned int padded_chunks = data_chunk_count - objsize / blocksize;

  // zero_elements is of size MAX_MATRIX*MAX_MATRIX
  memset(zero_elements, false, sizeof(zero_elements));

  if (padded_chunks) {
      // number of zero bytes in the last non-zero chunk, r from above
      unsigned int remainder = objsize - \
                               (data_chunk_count - padded_chunks) * blocksize;
      unsigned int rows = get_row_count();
      // number of zero rows in last chunk
      unsigned int zero_rows = ((blocksize - remainder) * rows) / blocksize;

      // set zero rows in last chunk
      // technically we should add zeroes in zero_rows + MAX_MATRIX - rows
      // but we'll leave these columns as junk.
      memset(&zero_elements[data_chunk_count - padded_chunks][rows - zero_rows],
             true, zero_rows);

      // set zero chunks for each padded chunk (could set MAX_MATRIX but we'll
      // leave the junk columns as zeroes)
      for (unsigned int i = data_chunk_count - padded_chunks + 1;
              i < data_chunk_count; i++) {
          memset(zero_elements[i], true, rows);
      }
      dout(1) << "MatanLiramV10: zero_elements padded_chunks: " << padded_chunks
        << ", rows per padded chunk: " << rows << ", zero_rows at last chunk: "
        << zero_rows << dendl;
  }

  return 0;
}

// raw is ordered by shard, encoded is ordered by chunk_mapping
int ErasureCodeZigzag::encode_prepare(const bufferlist &raw,
    map<int, bufferlist> &encoded)
{
  unsigned int k = get_data_chunk_count();
  unsigned int r = get_coding_chunk_count();
  unsigned blocksize = get_chunk_size(raw.length());
  dout(1) << "MatanLiramV8: blocksize: " << blocksize << dendl;
  unsigned padded_chunks = k - raw.length() / blocksize;
  dout(1) << "MatanLiramV8: padded chunks: " << padded_chunks << dendl;;
  bufferlist prepared = raw;
  dout(1) << "MatanLiramV8: raw length: " << raw.length() << dendl;

  for (unsigned int i = 0; i < k - padded_chunks; i++) {
    // chunk_index maps the i-th stripe to its chunk_index in the code
    bufferlist &chunk = encoded[chunk_index(i)];
    chunk.substr_of(prepared, i * blocksize, blocksize);
    chunk.rebuild_aligned_size_and_memory(blocksize, SIMD_ALIGN);
    assert(chunk.is_contiguous());
  }

  if (padded_chunks) {
    unsigned remainder = raw.length() - (k - padded_chunks) * blocksize;
    bufferptr buf(buffer::create_aligned(blocksize, SIMD_ALIGN));

    // copy remainder bytes of last chunk from raw to buf
    raw.copy((k - padded_chunks) * blocksize, remainder, buf.c_str());
    // the rest of buf is zeroes
    buf.zero(remainder, blocksize - remainder);
    encoded[chunk_index(k - padded_chunks)].push_back(std::move(buf));

    // from the first empty chunk to last, set a zero buffer in encoded
    for (unsigned int i = k - padded_chunks + 1; i < k; i++) {
      bufferptr buf(buffer::create_aligned(blocksize, SIMD_ALIGN));
      buf.zero();
      encoded[chunk_index(i)].push_back(std::move(buf));
    }
  }
  // add r junk bufferlists for parity
  for (unsigned int i = k; i < k + r; i++) {
    bufferlist &chunk = encoded[chunk_index(i)];
    chunk.push_back(buffer::create_aligned(blocksize, SIMD_ALIGN));
  }

  return 0;
}

// P comment means we try to remove things to fix parallel action
// in is ordered by OSD
// want_to_encode - includes shards 0 to k+r-1
// in - includes all data chunks
// encoded - empty before calling to encode
int ErasureCodeZigzag::encode(const set<int> &want_to_encode,
    const bufferlist &in,
    map<int, bufferlist> *encoded)
{
  //dout(1) << "encode" << dendl;
  unsigned k = get_data_chunk_count();
  unsigned r = get_coding_chunk_count();
  bufferlist out;

  // init_zigzag();

  int err = encode_prepare(in, *encoded);
  if (err)
    return err;

  err = encode_chunks(want_to_encode, encoded);
  for (unsigned i = 0; i < (unsigned) (k + r); i++) {
    if (want_to_encode.count(i) == 0)
      encoded->erase(i);
  }
  return err;
}


// P comment means we try to remove things to fix parallel action
// until now it was all ordered by shards
int ErasureCodeZigzag::encode_chunks(const set<int> &want_to_encode,
    map<int, bufferlist> *encoded)
{
  //dout(1) << "encode_chunks" << dendl;
  unsigned k = get_data_chunk_count();
  unsigned r = get_coding_chunk_count();
  // actually will be sinfo.get_chunk_size() but this way is good in case
  // we change sinfo.
  // should be ok since we call encode only when all data chunks are available
  // there's a small assumption that chunk_index doesn't map parity chunks to
  // position 0 which is true by chunk_index() definition
  unsigned chunk_size = (*encoded)[0].length();
  dout(1) << "MatanLiramV8: chunk size: " << chunk_size << dendl;
  unsigned zigzag_chunk_count = r - 1;
  char *chunks[k + r];

  for (unsigned index = 0; index < (unsigned) (k + r); index++) {
    // chunks array is sorted D..DP..P (data and then parity chunks)
    dout(1) << "MatanLiramV8: bl size of chunk " << index << " is " << (*encoded)[index].length()
      << dendl;
    // as defined in prepare_encode, encoded is ordered by chunk_index(shard_id)
    // and therefore chunks has the same order
    chunks[index] = (*encoded)[index].c_str();
  }

  //encode_chunks_parity(chunks, chunk_size);
  dout(1) << __func__ << ": Before do_parity" << dendl;
  do_parity(k, chunks, chunks[k], chunk_size);

  for (unsigned index = 0; index < zigzag_chunk_count; index++) {
    dout(1) << __func__ << ": Encoding zigzag chunk " << index << dendl;
    encode_chunks_zigzag(chunks, chunk_size, index);
  }

  return 0;
}

// same as jerasure_do_parity but we don't assume that the data is contigious.
void ErasureCodeZigzag::do_parity(int k, char *data_ptrs[], char *parity_ptr, int size) {
  int i;

  memcpy(parity_ptr, data_ptrs[0], size);
  for (i=1; i<k; ++i) {
    dout(1) << __func__ << ": galois region " << i << dendl;
    galois_region_xor(data_ptrs[i], parity_ptr, size);
  }
}

void ErasureCodeZigzag::galois_multiply(char *region, int multby, int nbytes, char *r2, int add)
{
    switch (get_field()) {
    case 8:
        galois_w08_region_multiply(region, multby, nbytes, r2, add);
        break;
    case 16:
        galois_w16_region_multiply(region, multby, nbytes, r2, add);
        break;
    case 32:
        galois_w32_region_multiply(region, multby, nbytes, r2, add);
        break;
    }
}

void ErasureCodeZigzag::encode_chunks_zigzag(char *chunks[],
    unsigned chunk_size,
    unsigned zz_chunk)
{
  unsigned int data_chunk_count = get_data_chunk_count();
  unsigned int row_count = get_row_count();
  unsigned int row_of_chunk_size = chunk_size / row_count;
  vector<int> init_rows(row_count);

  for (unsigned int chunk_index = 0; chunk_index < data_chunk_count; chunk_index++) {
    for (unsigned int row_index = 0; row_index < row_count; row_index++) {
      dout(1) << __func__ << ": Encoding chunk " << chunk_index << " row "
              << row_index << dendl;
      char *dst = chunks[get_data_chunk_count() + 1 + zz_chunk] + \
              (row_index * row_of_chunk_size);
      unsigned int zigzag_index = pZZ_G->zz_encode[zz_chunk][row_index][chunk_index];
      assert(zigzag_index < row_count);
      char *src = chunks[chunk_index] + (zigzag_index * row_of_chunk_size);
      if(zero_elements[chunk_index][zigzag_index] == true) {
        continue;
      }
      galois_multiply(src, pZZ_G->zz_coeff[zz_chunk][row_index][chunk_index],
          row_of_chunk_size, dst, init_rows[row_index]);
      init_rows[row_index] = 1;
    }
  }
}


int ErasureCodeZigzag::decode_chunks(const set<int> &want_to_read,
    const map<int, bufferlist> &chunks,
    map<int, bufferlist> *decoded)
{
  assert("ErasureCodeZigzag::decode_chunks not implemented" == 0);
}

int ErasureCodeZigzag::reconstruct_concat(const map<int, bufferlist> &chunks,
    bufferlist *reconstructed)
{
  dout(1) << "reconstruct_concat" << dendl;
  set<int> want_to_read;

  // MatanLiramV6: TODO: maybe we'd like to read only partial amounts of the chunk.
  // After the code fully works figure our how to enable that.
  for (unsigned i=0; i<get_data_chunk_count(); ++i) {
    want_to_read.insert(chunk_index(i));
  }
  map<int, bufferlist> reconstructed_map;
  int r = reconstruct(want_to_read, chunks, &reconstructed_map);
  if (r == 0) {
    for (unsigned i=0; i<get_data_chunk_count(); ++i) {
      reconstructed->claim_append(reconstructed_map[chunk_index(i)]);
    }
  }
  return r;
}

int ErasureCodeZigzag::reconstruct(const set<int> &chunks_to_read,
    const map<int, bufferlist> &chunks,
    map<int, bufferlist> *reconstructed)
{
  dout(1) << "reconstruct" << dendl;
  // int ret;
  vector<int> have;

  have.reserve(chunks.size());
  for (map<int, bufferlist>::const_iterator i = chunks.begin();
       i != chunks.end();
       ++i) {
    have.push_back(i->first);
  }
  // if we have what we need copy it to output and finish
  if (includes(
        have.begin(), have.end(), chunks_to_read.begin(), chunks_to_read.end())) {
    for (set<int>::iterator i = chunks_to_read.begin();
         i != chunks_to_read.end();
         ++i) {
      (*reconstructed)[*i] = chunks.find(*i)->second;
    }
    return 0;
  }

  // recovery process from now on...
  // chunks_to_read are only data chunks.

  /* No failures, 1 failure or more than 1 failure */
  unsigned k = get_data_chunk_count();
  unsigned r = get_coding_chunk_count();
  unsigned blocksize = chunks.begin()->second.length();
  // check that the chunk that we got in reconstruct is of the right size
  dout(1) << "MatanLiramV10: blocksize: " << blocksize << ",chunk_size: "
    << get_chunk_size(get_stripe_unit() * get_data_chunk_count()) << dendl;
  // asserts we put the right value in desired_stripe
  assert(blocksize == get_chunk_size(get_stripe_unit() * get_data_chunk_count()));
  for (unsigned i = 0; i < k + r; i++) {
    if (chunks.find(i) == chunks.end()) {
      bufferptr ptr(buffer::create_aligned(blocksize, SIMD_ALIGN));
      ptr.zero();
      (*reconstructed)[i].push_front(ptr);
    } else {
      (*reconstructed)[i] = chunks.find(i)->second;
    }
    (*reconstructed)[i].rebuild_aligned(SIMD_ALIGN);
  }

  return reconstruct_chunks(chunks_to_read, chunks, *reconstructed);
}

int ErasureCodeZigzag::reconstruct_chunks(const set<int> &want_to_read,
    const map<int, bufferlist> &read_chunks,
    map<int, bufferlist> &reconstructed)
{
  dout(1) << "reconstruct_chunks" << dendl;
  set<int> available_chunks;
  set<int> erasures;
  for (unsigned i = 0; i < get_chunk_count(); ++i) {
    if (read_chunks.count(i) != 0)
      available_chunks.insert(i);
    else
      erasures.insert(i);
  }
  dout(1) << "MatanLiramV10: num erasures: " << erasures.size() << ", min erasure"
    << (unsigned) *std::min_element(erasures.begin(), erasures.end()) << dendl;
  /*
   * assert(erasures.size() <= 1 \
   *    || (unsigned) *std::min_element(erasures.begin(), erasures.end()) \
   *      >= get_data_chunk_count());
   * up to r recoveries in case a parity failed
   */

  // assuming reconstruct_chunks is not called outisde erasure-code folder
  // because reconstructed map is not initialized when calling it separately...
  if (erasures.size() == 0) // copied to reconstructed in reconstruct
    return 0;
  else
    return reconstruct_chunks_aux(*(erasures.begin()), read_chunks, &reconstructed);
}

int ErasureCodeZigzag::reconstruct_chunks_aux(unsigned failed_chunk,
    const map<int, bufferlist> &read_chunks,
    map<int, bufferlist> *reconstructed)
{
  dout(1) << "reconstruct_chunks_aux" << dendl;
  unsigned chunk_count = get_chunk_count();
  unsigned chunk_size = (*reconstructed)[0].length();
  unsigned zigzag_chunk_count = get_coding_chunk_count() - 1;
  char *chunks[chunk_count];

  for (unsigned index = 0; index < chunk_count; index++) {
    chunks[index] = (*reconstructed)[index].c_str();
  }

  /* In case of parity chunk failure */
  // TODO: check if works fine (check if failed_chunk is from a sorted array of DDD..PPP)
  if (failed_chunk >= (unsigned) get_data_chunk_count()) {
    dout(1) << "MatanLiramV8: encoding all parity chunks during recovery"
       << dendl;
    do_parity(k, chunks, chunks[k], chunk_size);
    for (unsigned idx = 0; idx < zigzag_chunk_count; ++idx) {
      encode_chunks_zigzag(chunks, chunk_size, idx);
    }
    return 0;
  }
  /*if (failed_chunk == (unsigned) get_data_chunk_count()) {
    //encode_chunks_parity(chunks, chunk_size);
    do_parity(k, chunks, chunks[k], chunk_size);
    return 0;
  }

  // failed_chunk is not the chunk_index but the real chunk, so >= can work
  if (failed_chunk >= get_data_chunk_count() + 1) {
    encode_chunks_zigzag(chunks, chunk_size, failed_chunk - get_data_chunk_count() - 1);
    return 0;
  }*/
  dout(1) << "MatanLiramV8: data chunk recovery here" << dendl;
  reconstruct_chunks_parity(failed_chunk, read_chunks, chunks, chunk_size);

  for (unsigned index = 0; index < zigzag_chunk_count; index++) {
    reconstruct_chunks_zigzag(failed_chunk, read_chunks, chunks, chunk_size, index);
  }


  return 0;
}

int ErasureCodeZigzag::reconstruct_chunks_parity(unsigned failed_chunk,
    const map<int, bufferlist> &read_chunks,
    char *chunks[],
    unsigned chunk_size)
{
  unsigned data_chunk_count = get_data_chunk_count();
  unsigned row_count = get_row_count();
  unsigned row_parity_count = row_count / (get_chunk_count() - \
      get_data_chunk_count());
  unsigned row_of_chunk_size = chunk_size / row_count;

  for (unsigned index = 0; index < row_parity_count; index++) {
    unsigned row_index = pZZ_G->parity_rows[failed_chunk][index];
    char *src = chunks[get_data_chunk_count()] + (row_index * row_of_chunk_size);
    char *dst = chunks[failed_chunk] + (row_index * row_of_chunk_size);
    if(zero_elements[failed_chunk][row_index] == true) {
      continue;
    }
    memcpy(dst, src, row_of_chunk_size);
  }

  for (unsigned chunk_idx = 0; chunk_idx < data_chunk_count; chunk_idx++) {
    if (chunk_idx == failed_chunk) {
      continue;
    }

    for (unsigned index = 0; index < row_parity_count; index++) {
      unsigned row_index = pZZ_G->parity_rows[failed_chunk][index];
      char *src = chunks[chunk_idx] + (row_index * row_of_chunk_size);
      char *dst = chunks[failed_chunk] + (row_index * row_of_chunk_size);

      if(zero_elements[failed_chunk][row_index] == true || zero_elements[chunk_idx][row_index]) {
        continue;
      }

      galois_region_xor(src, dst, row_of_chunk_size);
    }
  }

  return 0;

}

int ErasureCodeZigzag::reconstruct_chunks_zigzag(unsigned failed_chunk,
    const map<int, bufferlist> &read_chunks,
    char *chunks[],
    unsigned chunk_size,
    unsigned zz_chunk)
{
  unsigned data_chunk_count = get_data_chunk_count();
  unsigned row_count = get_row_count();
  unsigned row_parity_count = row_count / (get_chunk_count() - get_data_chunk_count());
  unsigned row_of_chunk_size = chunk_size / row_count;
  unsigned zigzag_chunk_index = get_data_chunk_count() + 1 + zz_chunk;

  for (unsigned index = 0; index < row_parity_count; index++) {
    /* index of row of chunk that needs to reconstruct */
    unsigned row_index = pZZ_G->zigzag_rows[zz_chunk][failed_chunk][index];
    /* index of row of zigzag chunk we need for reconstruct */
    unsigned zigzag_row_index = pZZ_G->zz_perms[zz_chunk][failed_chunk][row_index];

    char *src = chunks[zigzag_chunk_index] + (zigzag_row_index * row_of_chunk_size);
    char *dst = chunks[failed_chunk] + (row_index * row_of_chunk_size);

    if(zero_elements[failed_chunk][row_index] == true) {
      continue;
    }

    memcpy(dst, src, row_of_chunk_size);
  }

  for (unsigned chunk_idx = 0; chunk_idx < data_chunk_count; chunk_idx++) {
    if (chunk_idx == failed_chunk) {
      continue;
    }

    for (unsigned index = 0; index < row_parity_count; index++) {
      /* the failed row of the chunk we want to reconstruct */
      unsigned row_index = pZZ_G->zigzag_rows[zz_chunk][failed_chunk][index];
      /* the zigzag row of the chunk we want to reconstruct */
      unsigned zigzag_row_index = pZZ_G->zz_perms[zz_chunk][failed_chunk][row_index];
      /* the zigzag row of the chunk we want to reconstruct */
      unsigned src_zigzag_index = pZZ_G->zz_encode[zz_chunk][zigzag_row_index][chunk_idx];
      /* the source row chunk of chunk_idx that is in zigzag_index */
      char *src = chunks[chunk_idx] + (src_zigzag_index * row_of_chunk_size);
      /* the destination row chunk of the chunk we want to reconstruct */
      char *dst = chunks[failed_chunk] + (row_index * row_of_chunk_size);

      if (zero_elements[chunk_idx][src_zigzag_index] == true
          || zero_elements[failed_chunk][row_index] == true) {
        continue;
      }

      galois_multiply(src, pZZ_G->zz_coeff[zz_chunk][zigzag_row_index][chunk_idx],
          row_of_chunk_size, dst, 1);
    }
  }

  for (unsigned index = 0; index < row_parity_count; index++) {
    unsigned row_index = pZZ_G->zigzag_rows[zz_chunk][failed_chunk][index];
    unsigned zigzag_index = pZZ_G->zz_perms[zz_chunk][failed_chunk][row_index];
    char *src = chunks[failed_chunk] + (row_index * row_of_chunk_size);

    if(zero_elements[failed_chunk][row_index] == true) {
      continue;
    }

    galois_multiply(src,
        zz_transposes[pZZ_G->zz_coeff[zz_chunk][zigzag_index][failed_chunk]],
        row_of_chunk_size,
        src,
        0);
  }


  return 0;
}

