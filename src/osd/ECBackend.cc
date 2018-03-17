// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <sstream>

#include "ECBackend.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "ECMsgTypes.h"

#include "PrimaryLogPG.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ECBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

struct ECRecoveryHandle : public PGBackend::RecoveryHandle {
  list<ECBackend::RecoveryOp> ops;
};

ostream &operator<<(ostream &lhs, const ECBackend::pipeline_state_t &rhs) {
  switch (rhs.pipeline_state) {
  case ECBackend::pipeline_state_t::CACHE_VALID:
    return lhs << "CACHE_VALID";
  case ECBackend::pipeline_state_t::CACHE_INVALID:
    return lhs << "CACHE_INVALID";
  default:
    assert(0 == "invalid pipeline state");
  }
  return lhs; // unreachable
}

static ostream &operator<<(ostream &lhs, const map<pg_shard_t, bufferlist> &rhs)
{
  lhs << "[";
  for (map<pg_shard_t, bufferlist>::const_iterator i = rhs.begin();
       i != rhs.end();
       ++i) {
    if (i != rhs.begin())
      lhs << ", ";
    lhs << make_pair(i->first, i->second.length());
  }
  return lhs << "]";
}

static ostream &operator<<(ostream &lhs, const map<int, bufferlist> &rhs)
{
  lhs << "[";
  for (map<int, bufferlist>::const_iterator i = rhs.begin();
       i != rhs.end();
       ++i) {
    if (i != rhs.begin())
      lhs << ", ";
    lhs << make_pair(i->first, i->second.length());
  }
  return lhs << "]";
}

static ostream &operator<<(
  ostream &lhs,
  const boost::tuple<uint64_t, uint64_t, map<pg_shard_t, bufferlist> > &rhs)
{
  return lhs << "(" << rhs.get<0>() << ", "
	     << rhs.get<1>() << ", " << rhs.get<2>() << ")";
}

ostream &operator<<(ostream &lhs, const ECBackend::read_request_t &rhs)
{
  return lhs << "read_request_t(to_read=[" << rhs.to_read << "]"
	     << ", need=" << rhs.need
	     << ", want_attrs=" << rhs.want_attrs
	     << ")";
}

ostream &operator<<(ostream &lhs, const ECBackend::read_result_t &rhs)
{
  lhs << "read_result_t(r=" << rhs.r
      << ", errors=" << rhs.errors;
  if (rhs.attrs) {
    lhs << ", attrs=" << rhs.attrs.get();
  } else {
    lhs << ", noattrs";
  }
  lhs << ", elements_map=" << rhs.elements_map.size();
  return lhs << ", returned=" << rhs.returned << ")";
}

ostream &operator<<(ostream &lhs, const ECBackend::ReadOp &rhs)
{
  lhs << "ReadOp(tid=" << rhs.tid;
  if (rhs.op && rhs.op->get_req()) {
    lhs << ", op=";
    rhs.op->get_req()->print(lhs);
  }
  return lhs << ", to_read=" << rhs.to_read
	     << ", complete=" << rhs.complete
	     << ", priority=" << rhs.priority
	     << ", obj_to_source=" << rhs.obj_to_source
	     << ", source_to_obj=" << rhs.source_to_obj
	     << ", in_progress=" << rhs.in_progress << ")";
}

void ECBackend::ReadOp::dump(Formatter *f) const
{
  f->dump_unsigned("tid", tid);
  if (op && op->get_req()) {
    f->dump_stream("op") << *(op->get_req());
  }
  f->dump_stream("to_read") << to_read;
  f->dump_stream("complete") << complete;
  f->dump_int("priority", priority);
  f->dump_stream("obj_to_source") << obj_to_source;
  f->dump_stream("source_to_obj") << source_to_obj;
  f->dump_stream("in_progress") << in_progress;
}

ostream &operator<<(ostream &lhs, const ECBackend::Op &rhs)
{
  lhs << "Op(" << rhs.hoid
      << " v=" << rhs.version
      << " tt=" << rhs.trim_to
      << " tid=" << rhs.tid
      << " reqid=" << rhs.reqid;
  if (rhs.client_op && rhs.client_op->get_req()) {
    lhs << " client_op=";
    rhs.client_op->get_req()->print(lhs);
  }
  lhs << " roll_forward_to=" << rhs.roll_forward_to
      << " temp_added=" << rhs.temp_added
      << " temp_cleared=" << rhs.temp_cleared
      << " pending_read=" << rhs.pending_read
      << " remote_read=" << rhs.remote_read
      << " remote_read_result=" << rhs.remote_read_result
      << " pending_apply=" << rhs.pending_apply
      << " pending_commit=" << rhs.pending_commit
      << " plan.to_read=" << rhs.plan.to_read
      << " plan.will_write=" << rhs.plan.will_write
      << ")";
  return lhs;
}

ostream &operator<<(ostream &lhs, const ECBackend::RecoveryOp &rhs)
{
  return lhs << "RecoveryOp("
	     << "hoid=" << rhs.hoid
	     << " v=" << rhs.v
	     << " missing_on=" << rhs.missing_on
	     << " missing_on_shards=" << rhs.missing_on_shards
	     << " recovery_info=" << rhs.recovery_info
	     << " recovery_progress=" << rhs.recovery_progress
	     << " obc refcount=" << rhs.obc.use_count()
	     << " state=" << ECBackend::RecoveryOp::tostr(rhs.state)
	     << " waiting_on_pushes=" << rhs.waiting_on_pushes
	     << " extent_requested=" << rhs.extent_requested
	     << ")";
}

void ECBackend::RecoveryOp::dump(Formatter *f) const
{
  f->dump_stream("hoid") << hoid;
  f->dump_stream("v") << v;
  f->dump_stream("missing_on") << missing_on;
  f->dump_stream("missing_on_shards") << missing_on_shards;
  f->dump_stream("recovery_info") << recovery_info;
  f->dump_stream("recovery_progress") << recovery_progress;
  f->dump_stream("state") << tostr(state);
  f->dump_stream("waiting_on_pushes") << waiting_on_pushes;
  f->dump_stream("extent_requested") << extent_requested;
}

ECBackend::ECBackend(
  PGBackend::Listener *pg,
  coll_t coll,
  ObjectStore::CollectionHandle &ch,
  ObjectStore *store,
  CephContext *cct,
  ErasureCodeInterfaceRef ec_impl,
  uint64_t stripe_width)
  : PGBackend(cct, pg, store, coll, ch),
    ec_impl(ec_impl),
    sinfo(ec_impl->get_data_chunk_count(), stripe_width, ec_impl->get_row_count(),
        ec_impl->get_zigzag_duplication()) {
  if (cct->_conf->get_val<std::string>("osd_pool_recovery_read_type") == "trivial")
  {
    read_type = ReadType::Trivial;
  }
  else if (cct->_conf->get_val<std::string>("osd_pool_recovery_read_type") == "aggressive")
  {
    read_type = ReadType::Aggressive;
  }
  else if (cct->_conf->get_val<std::string>("osd_pool_recovery_read_type") == "conservative")
  {
    read_type = ReadType::Conservative;
  }
  else
  {
    read_type = ReadType::Conservative;
  }
  assert((ec_impl->get_data_chunk_count() *
	  ec_impl->get_chunk_size(stripe_width)) == stripe_width);
}

PGBackend::RecoveryHandle *ECBackend::open_recovery_op()
{
  return new ECRecoveryHandle;
}

void ECBackend::_failed_push(const hobject_t &hoid,
  pair<RecoveryMessages *, ECBackend::read_result_t &> &in)
{
  ECBackend::read_result_t &res = in.second;
  dout(10) << __func__ << ": Read error " << hoid << " r="
	   << res.r << " errors=" << res.errors << dendl;
  dout(10) << __func__ << ": canceling recovery op for obj " << hoid
	   << dendl;
  assert(recovery_ops.count(hoid));
  recovery_ops.erase(hoid);

  list<pg_shard_t> fl;
  for (auto&& i : res.errors) {
    fl.push_back(i.first);
  }
  get_parent()->failed_push(fl, hoid);
}

struct OnRecoveryReadComplete :
  public GenContext<pair<RecoveryMessages*, ECBackend::read_result_t& > &> {
  ECBackend *pg;
  hobject_t hoid;
  set<int> want;
  OnRecoveryReadComplete(ECBackend *pg, const hobject_t &hoid)
    : pg(pg), hoid(hoid) {}
  void finish(pair<RecoveryMessages *, ECBackend::read_result_t &> &in) override {
    ECBackend::read_result_t &res = in.second;
    if (!(res.r == 0 && res.errors.empty())) {
        pg->_failed_push(hoid, in);
        return;
    }
    assert(res.returned.size() == 1);
    pg->handle_recovery_read_complete(
      hoid,
      res.returned.back(),
      res.attrs,
      in.first,
      res.elements_map);
  }
};

struct RecoveryMessages {
  map<hobject_t,
      ECBackend::read_request_t> reads;
  void read(
    ECBackend *ec,
    const hobject_t &hoid, uint64_t off, uint64_t len,
    const set<pg_shard_t> &need,
    bool attrs) {
    list<boost::tuple<uint64_t, uint64_t, uint32_t> > to_read;
    to_read.push_back(boost::make_tuple(off, len, 0));
    assert(!reads.count(hoid));
    reads.insert(
      make_pair(
	hoid,
	ECBackend::read_request_t(
	  to_read,
	  need,
	  attrs,
	  new OnRecoveryReadComplete(
	    ec,
	    hoid))));
  }

  void read( // Overloading
    ECBackend *ec,
    const hobject_t &hoid, uint64_t off, uint64_t len,
    const set<pg_shard_t> &need,
    bool attrs,
    const map<int, set<int> > &elements_map) {
    list<boost::tuple<uint64_t, uint64_t, uint32_t> > to_read;
    to_read.push_back(boost::make_tuple(off, len, 0));
    assert(!reads.count(hoid));
    reads.insert(
      make_pair(
	hoid,
	ECBackend::read_request_t(
	  to_read,
	  need,
	  attrs,
	  new OnRecoveryReadComplete(
	    ec,
	    hoid),
          elements_map)));
  }

  map<pg_shard_t, vector<PushOp> > pushes;
  map<pg_shard_t, vector<PushReplyOp> > push_replies;
  ObjectStore::Transaction t;
  RecoveryMessages() {}
  ~RecoveryMessages(){}
};

void ECBackend::handle_recovery_push(
  const PushOp &op,
  RecoveryMessages *m)
{
  ostringstream ss;
  if (get_parent()->check_failsafe_full(ss)) {
    dout(10) << __func__ << " Out of space (failsafe) processing push request: " << ss.str() << dendl;
    ceph_abort();
  }

  bool oneshot = op.before_progress.first && op.after_progress.data_complete;
  ghobject_t tobj;
  if (oneshot) {
    tobj = ghobject_t(op.soid, ghobject_t::NO_GEN,
		      get_parent()->whoami_shard().shard);
  } else {
    tobj = ghobject_t(get_parent()->get_temp_recovery_object(op.soid,
							     op.version),
		      ghobject_t::NO_GEN,
		      get_parent()->whoami_shard().shard);
    if (op.before_progress.first) {
      dout(10) << __func__ << ": Adding oid "
	       << tobj.hobj << " in the temp collection" << dendl;
      add_temp_obj(tobj.hobj);
    }
  }

  if (op.before_progress.first) {
    m->t.remove(coll, tobj);
    m->t.touch(coll, tobj);
  }

  if (!op.data_included.empty()) {
    uint64_t start = op.data_included.range_start();
    uint64_t end = op.data_included.range_end();
    assert(op.data.length() == (end - start));

    m->t.write(
      coll,
      tobj,
      start,
      op.data.length(),
      op.data);
  } else {
    assert(op.data.length() == 0);
  }

  if (op.before_progress.first) {
    assert(op.attrset.count(string("_")));
    m->t.setattrs(
      coll,
      tobj,
      op.attrset);
  }

  if (op.after_progress.data_complete && !oneshot) {
    dout(10) << __func__ << ": Removing oid "
	     << tobj.hobj << " from the temp collection" << dendl;
    clear_temp_obj(tobj.hobj);
    m->t.remove(coll, ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
    m->t.collection_move_rename(
      coll, tobj,
      coll, ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  }
  if (op.after_progress.data_complete) {
    if ((get_parent()->pgb_is_primary())) {
      assert(recovery_ops.count(op.soid));
      assert(recovery_ops[op.soid].obc);
      get_parent()->on_local_recover(
	op.soid,
	op.recovery_info,
	recovery_ops[op.soid].obc,
	false,
	&m->t);
    } else {
      get_parent()->on_local_recover(
	op.soid,
	op.recovery_info,
	ObjectContextRef(),
	false,
	&m->t);
    }
  }
  m->push_replies[get_parent()->primary_shard()].push_back(PushReplyOp());
  m->push_replies[get_parent()->primary_shard()].back().soid = op.soid;
}

void ECBackend::handle_recovery_push_reply(
  const PushReplyOp &op,
  pg_shard_t from,
  RecoveryMessages *m)
{
  if (!recovery_ops.count(op.soid))
    return;
  RecoveryOp &rop = recovery_ops[op.soid];
  assert(rop.waiting_on_pushes.count(from));
  rop.waiting_on_pushes.erase(from);
  continue_recovery_op(rop, m);
}

void ECBackend::handle_recovery_read_complete(
  const hobject_t &hoid,
  boost::tuple<uint64_t, uint64_t, map<pg_shard_t, bufferlist> > &to_read,
  boost::optional<map<string, bufferlist> > attrs,
  RecoveryMessages *m,
  const map<int, set<int> > &elements_map)
{
  dout(10) << __func__ << ": returned " << hoid << " "
	   << "(" << to_read.get<0>()
	   << ", " << to_read.get<1>()
	   << ", " << to_read.get<2>()
	   << ")"
	   << dendl;
  assert(recovery_ops.count(hoid));
  RecoveryOp &op = recovery_ops[hoid];
  assert(op.returned_data.empty());
  map<int, bufferlist*> target;
  for (set<shard_id_t>::iterator i = op.missing_on_shards.begin();
       i != op.missing_on_shards.end();
       ++i) {
    target[*i] = &(op.returned_data[*i]);
  }
  uint64_t element_size = sinfo.get_chunk_size() / sinfo.get_row_count();
  assert(sinfo.get_chunk_size() % sinfo.get_row_count() == 0);
  map<int, bufferlist> from;
  // The to_read is "returned" of read_result_t. We iterate on the
  // concatenated bufferlist from each element.
  for(map<pg_shard_t, bufferlist>::iterator i = to_read.get<2>().begin();
      i != to_read.get<2>().end();
      ++i) {
    if (elements_map.size() == 0) {
      from[i->first.shard].claim(i->second);
    } else if (elements_map.find(i->first.shard) == elements_map.end()) {
      dout(1) << __func__ << "got the buffer of parity shard " << i->first.shard
          << " of size " << i->second << ", appending zeros" << dendl;
      from[i->first.shard].append_zero(sinfo.get_chunk_size());
    } else { // elements_map is relevant to this chunk
      dout(1) << __func__ << "length of bufferlist received (" << 
          i->first.shard << ") is " << i->second.length() << dendl;
      // risky change here
      from[i->first.shard].clear();
      assert(elements_map.find(i->first.shard) != elements_map.end());
      const set<int> &elements = elements_map.find(i->first.shard)->second;
      int prev_element = 0;
      int count = 0; // element num
      for (set<int>::const_iterator j = elements.begin(); j != elements.end();
          ++j, ++count) {
        // number of zero element blocks needed to add to decode
        const int zero_size = *j - prev_element;
        dout(1) << __func__ << "zero size: " << zero_size << dendl;
        // we don't nullify the element itself, therefore the +1 is important
        prev_element = *j+1;
        // append zeros if needed:
        if (zero_size > 0) {
          from[i->first.shard].append_zero(element_size * zero_size);
        }
        bufferlist subbuf;
        subbuf.substr_of(i->second, element_size * count, element_size);
        dout(1) << __func__ << "subbuf size: " << subbuf.length() << dendl;
        from[i->first.shard].claim_append(subbuf); // append 1 more element
        // TODO: optimize to append more than 1 element if possible.
      }
      // zeroes up to the last row
      if (sinfo.get_row_count() - prev_element > 0) {
        from[i->first.shard].append_zero(
            element_size * (sinfo.get_row_count() - prev_element));
      }
      assert(from[i->first.shard].length() == sinfo.get_chunk_size());
    }
  } // end of chunk iteration for loop
  dout(10) << __func__ << ": " << from << dendl;
  int r = ECUtil::decode(sinfo, ec_impl, from, target);
  assert(r == 0);
  if (attrs) {
    op.xattrs.swap(*attrs);

    if (!op.obc) {
      // attrs only reference the origin bufferlist (decode from
      // ECSubReadReply message) whose size is much greater than attrs
      // in recovery. If obc cache it (get_obc maybe cache the attr),
      // this causes the whole origin bufferlist would not be free
      // until obc is evicted from obc cache. So rebuild the
      // bufferlist before cache it.
      for (map<string, bufferlist>::iterator it = op.xattrs.begin();
           it != op.xattrs.end();
           ++it) {
        it->second.rebuild();
      }
      // Need to remove ECUtil::get_hinfo_key() since it should not leak out
      // of the backend (see bug #12983)
      map<string, bufferlist> sanitized_attrs(op.xattrs);
      sanitized_attrs.erase(ECUtil::get_hinfo_key());
      op.obc = get_parent()->get_obc(hoid, sanitized_attrs);
      assert(op.obc);
      op.recovery_info.size = op.obc->obs.oi.size;
      op.recovery_info.oi = op.obc->obs.oi;
    }

    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    if (op.obc->obs.oi.size > 0) {
      assert(op.xattrs.count(ECUtil::get_hinfo_key()));
      bufferlist::iterator bp = op.xattrs[ECUtil::get_hinfo_key()].begin();
      ::decode(hinfo, bp);
    }
    op.hinfo = unstable_hashinfo_registry.lookup_or_create(hoid, hinfo);
  }
  assert(op.xattrs.size());
  assert(op.obc);
  continue_recovery_op(op, m);
}

struct SendPushReplies : public Context {
  PGBackend::Listener *l;
  epoch_t epoch;
  map<int, MOSDPGPushReply*> replies;
  SendPushReplies(
    PGBackend::Listener *l,
    epoch_t epoch,
    map<int, MOSDPGPushReply*> &in) : l(l), epoch(epoch) {
    replies.swap(in);
  }
  void finish(int) override {
    for (map<int, MOSDPGPushReply*>::iterator i = replies.begin();
	 i != replies.end();
	 ++i) {
      l->send_message_osd_cluster(i->first, i->second, epoch);
    }
    replies.clear();
  }
  ~SendPushReplies() override {
    for (map<int, MOSDPGPushReply*>::iterator i = replies.begin();
	 i != replies.end();
	 ++i) {
      i->second->put();
    }
    replies.clear();
  }
};

void ECBackend::dispatch_recovery_messages(RecoveryMessages &m, int priority)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = m.pushes.begin();
       i != m.pushes.end();
       m.pushes.erase(i++)) {
    MOSDPGPush *msg = new MOSDPGPush();
    msg->set_priority(priority);
    msg->map_epoch = get_parent()->get_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->pushes.swap(i->second);
    msg->compute_cost(cct);
    get_parent()->send_message(
      i->first.osd,
      msg);
  }
  map<int, MOSDPGPushReply*> replies;
  for (map<pg_shard_t, vector<PushReplyOp> >::iterator i =
	 m.push_replies.begin();
       i != m.push_replies.end();
       m.push_replies.erase(i++)) {
    MOSDPGPushReply *msg = new MOSDPGPushReply();
    msg->set_priority(priority);
    msg->map_epoch = get_parent()->get_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->replies.swap(i->second);
    msg->compute_cost(cct);
    replies.insert(make_pair(i->first.osd, msg));
  }

  if (!replies.empty()) {
    (m.t).register_on_complete(
	get_parent()->bless_context(
	  new SendPushReplies(
	    get_parent(),
	    get_parent()->get_epoch(),
	    replies)));
    get_parent()->queue_transaction(std::move(m.t));
  } 

  if (m.reads.empty())
    return;
  start_read_op(
    priority,
    m.reads,
    OpRequestRef(),
    false, true);
}

void ECBackend::continue_recovery_op(
  RecoveryOp &op,
  RecoveryMessages *m)
{
  dout(10) << __func__ << ": continuing " << op << dendl;
  while (1) {
    switch (op.state) {
    case RecoveryOp::IDLE: {
      // start read
      op.state = RecoveryOp::READING;
      assert(!op.recovery_progress.data_complete);
      set<int> want(op.missing_on_shards.begin(), op.missing_on_shards.end());
      uint64_t from = op.recovery_progress.data_recovered_to;
      uint64_t amount = get_recovery_chunk_size();
      map<int, set<int> > elements_map = map<int, set<int> >();

      if (op.recovery_progress.first && op.obc) {
	/* We've got the attrs and the hinfo, might as well use them */
	op.hinfo = get_hash_info(op.hoid);
	assert(op.hinfo);
	op.xattrs = op.obc->attr_cache;
	::encode(*(op.hinfo), op.xattrs[ECUtil::get_hinfo_key()]);
      }

      set<pg_shard_t> to_read;
      int r = get_min_avail_to_read_shards(
	op.hoid, want, true, false, &to_read, elements_map);
      if (r != 0) {
	// we must have lost a recovery source
	assert(!op.recovery_progress.first);
	dout(10) << __func__ << ": canceling recovery op for obj " << op.hoid
		 << dendl;
	get_parent()->cancel_pull(op.hoid);
	recovery_ops.erase(op.hoid);
	return;
      }
      m->read(
	this,
	op.hoid,
	op.recovery_progress.data_recovered_to,
	amount,
	to_read,
	op.recovery_progress.first && !op.obc,
        elements_map);
      // Used in READING state, to say that we finished recovering all the
      // stripes in length of recovery_max_chunk.
      op.extent_requested = make_pair(
	from,
	amount);
      dout(10) << __func__ << ": IDLE return " << op << dendl;
      return;
    }
    case RecoveryOp::READING: {
      // read completed, start write
      assert(op.xattrs.size());
      assert(op.returned_data.size());
      op.state = RecoveryOp::WRITING;
      ObjectRecoveryProgress after_progress = op.recovery_progress;
      // Extent_requested.second is always recovery_max_chunk
      // which is some multiple of the stripe size.
      after_progress.data_recovered_to += op.extent_requested.second;
      after_progress.first = false;
      if (after_progress.data_recovered_to >= op.obc->obs.oi.size) {
	after_progress.data_recovered_to =
	  sinfo.logical_to_next_stripe_offset(
	    op.obc->obs.oi.size);
	after_progress.data_complete = true;
      }
      // Iterate over shards we can recover from (on and missing)
      for (set<pg_shard_t>::iterator mi = op.missing_on.begin();
	   mi != op.missing_on.end();
	   ++mi) {
	assert(op.returned_data.count(mi->shard));
	m->pushes[*mi].push_back(PushOp()); // write op to all missing shards
	PushOp &pop = m->pushes[*mi].back();
	pop.soid = op.hoid;
	pop.version = op.v;
	pop.data = op.returned_data[mi->shard];
	dout(10) << __func__ << ": before_progress=" << op.recovery_progress
		 << ", after_progress=" << after_progress
		 << ", pop.data.length()=" << pop.data.length()
		 << ", size=" << op.obc->obs.oi.size << dendl;
	assert(
	  pop.data.length() ==
	  sinfo.aligned_logical_offset_to_chunk_offset(
	    after_progress.data_recovered_to -
	    op.recovery_progress.data_recovered_to)
	  );
	if (pop.data.length())
	  pop.data_included.insert(
	    sinfo.aligned_logical_offset_to_chunk_offset(
	      op.recovery_progress.data_recovered_to),
	    pop.data.length()
	    );
	if (op.recovery_progress.first) {
	  pop.attrset = op.xattrs;
	}
	pop.recovery_info = op.recovery_info;
	pop.before_progress = op.recovery_progress;
	pop.after_progress = after_progress;
        // If not primary, begin_peer_recover in primary,
	if (*mi != get_parent()->primary_shard())
	  get_parent()->begin_peer_recover(
	    *mi,
	    op.hoid);
      }
      op.returned_data.clear();
      op.waiting_on_pushes = op.missing_on;
      op.recovery_progress = after_progress;
      dout(10) << __func__ << ": READING return " << op << dendl;
      return;
    }
    case RecoveryOp::WRITING: {
      if (op.waiting_on_pushes.empty()) {
	if (op.recovery_progress.data_complete) {
	  op.state = RecoveryOp::COMPLETE;
	  for (set<pg_shard_t>::iterator i = op.missing_on.begin();
	       i != op.missing_on.end();
	       ++i) {
	    if (*i != get_parent()->primary_shard()) {
	      dout(10) << __func__ << ": on_peer_recover on " << *i
		       << ", obj " << op.hoid << dendl;
	      get_parent()->on_peer_recover(
		*i,
		op.hoid,
		op.recovery_info);
	    }
	  }
	  object_stat_sum_t stat;
	  stat.num_bytes_recovered = op.recovery_info.size;
	  stat.num_keys_recovered = 0; // ??? op ... omap_entries.size(); ?
	  stat.num_objects_recovered = 1;
	  get_parent()->on_global_recover(op.hoid, stat, false);
	  dout(10) << __func__ << ": WRITING return " << op << dendl;
	  recovery_ops.erase(op.hoid);
	  return;
	} else {
	  op.state = RecoveryOp::IDLE;
	  dout(10) << __func__ << ": WRITING continue " << op << dendl;
	  continue;
	}
      } // otherwise still waiting for PushOps!
      return;
    }
    // should never be called once complete
    case RecoveryOp::COMPLETE:
    default: {
      ceph_abort();
    };
    }
  }
}

void ECBackend::run_recovery_op(
  RecoveryHandle *_h,
  int priority)
{
  ECRecoveryHandle *h = static_cast<ECRecoveryHandle*>(_h);
  RecoveryMessages m;
  for (list<RecoveryOp>::iterator i = h->ops.begin();
       i != h->ops.end();
       ++i) {
    dout(10) << __func__ << ": starting " << *i << dendl;
    assert(!recovery_ops.count(i->hoid));
    RecoveryOp &op = recovery_ops.insert(make_pair(i->hoid, *i)).first->second;
    continue_recovery_op(op, &m);
  }

  dispatch_recovery_messages(m, priority);
  send_recovery_deletes(priority, h->deletes);
  delete _h;
}

int ECBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h)
{
  ECRecoveryHandle *h = static_cast<ECRecoveryHandle*>(_h);
  h->ops.push_back(RecoveryOp());
  h->ops.back().v = v;
  h->ops.back().hoid = hoid;
  h->ops.back().obc = obc;
  h->ops.back().recovery_info.soid = hoid;
  h->ops.back().recovery_info.version = v;
  if (obc) {
    h->ops.back().recovery_info.size = obc->obs.oi.size;
    h->ops.back().recovery_info.oi = obc->obs.oi;
  }
  if (hoid.is_snap()) {
    if (obc) {
      assert(obc->ssc);
      h->ops.back().recovery_info.ss = obc->ssc->snapset;
    } else if (head) {
      assert(head->ssc);
      h->ops.back().recovery_info.ss = head->ssc->snapset;
    } else {
      assert(0 == "neither obc nor head set for a snap object");
    }
  }
  h->ops.back().recovery_progress.omap_complete = true;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    dout(10) << "checking " << *i << dendl;
    if (get_parent()->get_shard_missing(*i).is_missing(hoid)) {
      h->ops.back().missing_on.insert(*i);
      h->ops.back().missing_on_shards.insert(i->shard);
    }
  }
  dout(10) << __func__ << ": built op " << h->ops.back() << dendl;
  return 0;
}

bool ECBackend::can_handle_while_inactive(
  OpRequestRef _op)
{
  return false;
}

bool ECBackend::_handle_message(
  OpRequestRef _op)
{
  dout(10) << __func__ << ": " << *_op->get_req() << dendl;
  int priority = _op->get_req()->get_priority();
  switch (_op->get_req()->get_type()) {
  case MSG_OSD_EC_WRITE: {
    // NOTE: this is non-const because handle_sub_write modifies the embedded
    // ObjectStore::Transaction in place (and then std::move's it).  It does
    // not conflict with ECSubWrite's operator<<.
    MOSDECSubOpWrite *op = static_cast<MOSDECSubOpWrite*>(
      _op->get_nonconst_req());
    handle_sub_write(op->op.from, _op, op->op, _op->pg_trace);
    return true;
  }
  case MSG_OSD_EC_WRITE_REPLY: {
    const MOSDECSubOpWriteReply *op = static_cast<const MOSDECSubOpWriteReply*>(
      _op->get_req());
    handle_sub_write_reply(op->op.from, op->op, _op->pg_trace);
    return true;
  }
  case MSG_OSD_EC_READ: {
    const MOSDECSubOpRead *op = static_cast<const MOSDECSubOpRead*>(_op->get_req());
    MOSDECSubOpReadReply *reply = new MOSDECSubOpReadReply;
    reply->pgid = get_parent()->primary_spg_t();
    reply->map_epoch = get_parent()->get_epoch();
    reply->min_epoch = get_parent()->get_interval_start_epoch();
    handle_sub_read(op->op.from, op->op, &(reply->op), _op->pg_trace);
    reply->trace = _op->pg_trace;
    // Notice that not all OSDs receive EC read message, id is decided here
    get_parent()->send_message_osd_cluster(
      op->op.from.osd, reply, get_parent()->get_epoch());
    return true;
  }
  case MSG_OSD_EC_READ_REPLY: {
    // NOTE: this is non-const because handle_sub_read_reply steals resulting
    // buffers.  It does not conflict with ECSubReadReply operator<<.
    MOSDECSubOpReadReply *op = static_cast<MOSDECSubOpReadReply*>(
      _op->get_nonconst_req());
    RecoveryMessages rm;
    handle_sub_read_reply(op->op.from, op->op, &rm, _op->pg_trace);
    dispatch_recovery_messages(rm, priority);
    return true;
  }
  case MSG_OSD_PG_PUSH: {
    const MOSDPGPush *op = static_cast<const MOSDPGPush *>(_op->get_req());
    RecoveryMessages rm;
    for (vector<PushOp>::const_iterator i = op->pushes.begin();
	 i != op->pushes.end();
	 ++i) {
      handle_recovery_push(*i, &rm);
    }
    dispatch_recovery_messages(rm, priority);
    return true;
  }
  case MSG_OSD_PG_PUSH_REPLY: {
    const MOSDPGPushReply *op = static_cast<const MOSDPGPushReply *>(
      _op->get_req());
    RecoveryMessages rm;
    for (vector<PushReplyOp>::const_iterator i = op->replies.begin();
	 i != op->replies.end();
	 ++i) {
      handle_recovery_push_reply(*i, op->from, &rm);
    }
    dispatch_recovery_messages(rm, priority);
    return true;
  }
  default:
    return false;
  }
  return false;
}

struct SubWriteCommitted : public Context {
  ECBackend *pg;
  OpRequestRef msg;
  ceph_tid_t tid;
  eversion_t version;
  eversion_t last_complete;
  const ZTracer::Trace trace;
  SubWriteCommitted(
    ECBackend *pg,
    OpRequestRef msg,
    ceph_tid_t tid,
    eversion_t version,
    eversion_t last_complete,
    const ZTracer::Trace &trace)
    : pg(pg), msg(msg), tid(tid),
      version(version), last_complete(last_complete), trace(trace) {}
  void finish(int) override {
    if (msg)
      msg->mark_event("sub_op_committed");
    pg->sub_write_committed(tid, version, last_complete, trace);
  }
};
void ECBackend::sub_write_committed(
  ceph_tid_t tid, eversion_t version, eversion_t last_complete,
  const ZTracer::Trace &trace) {
  if (get_parent()->pgb_is_primary()) {
    ECSubWriteReply reply;
    reply.tid = tid;
    reply.last_complete = last_complete;
    reply.committed = true;
    reply.from = get_parent()->whoami_shard();
    handle_sub_write_reply(
      get_parent()->whoami_shard(),
      reply, trace);
  } else {
    get_parent()->update_last_complete_ondisk(last_complete);
    MOSDECSubOpWriteReply *r = new MOSDECSubOpWriteReply;
    r->pgid = get_parent()->primary_spg_t();
    r->map_epoch = get_parent()->get_epoch();
    r->min_epoch = get_parent()->get_interval_start_epoch();
    r->op.tid = tid;
    r->op.last_complete = last_complete;
    r->op.committed = true;
    r->op.from = get_parent()->whoami_shard();
    r->set_priority(CEPH_MSG_PRIO_HIGH);
    r->trace = trace;
    r->trace.event("sending sub op commit");
    get_parent()->send_message_osd_cluster(
      get_parent()->primary_shard().osd, r, get_parent()->get_epoch());
  }
}

struct SubWriteApplied : public Context {
  ECBackend *pg;
  OpRequestRef msg;
  ceph_tid_t tid;
  eversion_t version;
  const ZTracer::Trace trace;
  SubWriteApplied(
    ECBackend *pg,
    OpRequestRef msg,
    ceph_tid_t tid,
    eversion_t version,
    const ZTracer::Trace &trace)
    : pg(pg), msg(msg), tid(tid), version(version), trace(trace) {}
  void finish(int) override {
    if (msg)
      msg->mark_event("sub_op_applied");
    pg->sub_write_applied(tid, version, trace);
  }
};
void ECBackend::sub_write_applied(
  ceph_tid_t tid, eversion_t version,
  const ZTracer::Trace &trace) {
  parent->op_applied(version);
  if (get_parent()->pgb_is_primary()) {
    ECSubWriteReply reply;
    reply.from = get_parent()->whoami_shard();
    reply.tid = tid;
    reply.applied = true;
    handle_sub_write_reply(
      get_parent()->whoami_shard(),
      reply, trace);
  } else {
    MOSDECSubOpWriteReply *r = new MOSDECSubOpWriteReply;
    r->pgid = get_parent()->primary_spg_t();
    r->map_epoch = get_parent()->get_epoch();
    r->min_epoch = get_parent()->get_interval_start_epoch();
    r->op.from = get_parent()->whoami_shard();
    r->op.tid = tid;
    r->op.applied = true;
    r->set_priority(CEPH_MSG_PRIO_HIGH);
    r->trace = trace;
    r->trace.event("sending sub op apply");
    get_parent()->send_message_osd_cluster(
      get_parent()->primary_shard().osd, r, get_parent()->get_epoch());
  }
}

void ECBackend::handle_sub_write(
  pg_shard_t from,
  OpRequestRef msg,
  ECSubWrite &op,
  const ZTracer::Trace &trace,
  Context *on_local_applied_sync)
{
  if (msg)
    msg->mark_started();
  trace.event("handle_sub_write");
  assert(!get_parent()->get_log().get_missing().is_missing(op.soid));
  if (!get_parent()->pgb_is_primary())
    get_parent()->update_stats(op.stats);
  ObjectStore::Transaction localt;
  if (!op.temp_added.empty()) {
    add_temp_objs(op.temp_added);
  }
  if (op.backfill) {
    for (set<hobject_t>::iterator i = op.temp_removed.begin();
	 i != op.temp_removed.end();
	 ++i) {
      dout(10) << __func__ << ": removing object " << *i
	       << " since we won't get the transaction" << dendl;
      localt.remove(
	coll,
	ghobject_t(
	  *i,
	  ghobject_t::NO_GEN,
	  get_parent()->whoami_shard().shard));
    }
  }
  clear_temp_objs(op.temp_removed);
  get_parent()->log_operation(
    op.log_entries,
    op.updated_hit_set_history,
    op.trim_to,
    op.roll_forward_to,
    !op.backfill,
    localt);

  PrimaryLogPG *_rPG = dynamic_cast<PrimaryLogPG *>(get_parent());
  if (_rPG && !_rPG->is_undersized() &&
      (unsigned)get_parent()->whoami_shard().shard >= ec_impl->get_data_chunk_count())
    op.t.set_fadvise_flag(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);

  if (on_local_applied_sync) {
    dout(10) << "Queueing onreadable_sync: " << on_local_applied_sync << dendl;
    localt.register_on_applied_sync(on_local_applied_sync);
  }
  localt.register_on_commit(
    get_parent()->bless_context(
      new SubWriteCommitted(
	this, msg, op.tid,
	op.at_version,
	get_parent()->get_info().last_complete, trace)));
  localt.register_on_applied(
    get_parent()->bless_context(
      new SubWriteApplied(this, msg, op.tid, op.at_version, trace)));
  vector<ObjectStore::Transaction> tls;
  tls.reserve(2);
  tls.push_back(std::move(op.t));
  tls.push_back(std::move(localt));
  get_parent()->queue_transactions(tls, msg);
}

template <typename MapIterator>
bool ECBackend::handle_aggressive_sub_read(
  pg_shard_t from,
  const ECSubRead &op,
  ECSubReadReply *reply,
  const ZTracer::Trace &trace,
  MapIterator &i,
  int &r,
  shard_id_t &shard,
  ECUtil::HashInfoRef &hinfo)
{
  // If list is not empty:
  if (i->second.begin() == i->second.end()) {
    return true;
  }

  uint64_t aggressive_off = i->second.front().template get<0>();
  // works well for reads of size 1
  const boost::tuple<uint64_t, uint64_t, uint32_t>& list_back = i->second.back();
  uint64_t aggressive_len = list_back.get<0>() + list_back.get<1>() \
                            - aggressive_off;
  bufferlist tmpbl;
  // read all at once from disk
  if (aggressive_len > 0) {
    r = store->read(
        ch, // ObjectStore::CollectionHandle
        ghobject_t(i->first, ghobject_t::NO_GEN, shard),
        aggressive_off,
        aggressive_len,
        tmpbl, list_back.get<2>()); // Allow EIO return
  } else {
    r = 0;
  }
  if (r < 0) {
    get_parent()->clog_error() << __func__
      << ": Error " << r
      << " reading "
      << i->first;
    dout(5) << __func__ << ": Error " << r
      << " reading " << i->first << dendl;
    return true;
  } else {
    // send what you got principle, if we got conservative to_read we
    // reply conservatively, otherwise we reply trivially (lots of messages).
    for (auto j =
        i->second.begin(); j != i->second.end(); ++j) {
      bufferlist bl;
      bl.substr_of(tmpbl, j->template get<0>() - aggressive_off, j->template get<1>());
      dout(20) << __func__ << " read request=" << j->template get<1>() << " r=" << r
        << " len=" << bl.length() << dendl;
      reply->buffers_read[i->first].push_back(
          make_pair(
            j->template get<0>(),
            bl)
          );
      if (!get_parent()->get_pool().allows_ecoverwrites()) {
        // This shows that we still need deep scrub because large enough files
        // are read in sections, so the digest check here won't be done here.
        // Do NOT check osd_read_eio_on_bad_digest here.  We need to report
        // the state of our chunk in case other chunks could substitute.
        assert(hinfo->has_chunk_hash());
        if ((bl.length() == hinfo->get_total_chunk_size()) &&
            (j->template get<0>() == 0)) {
          dout(20) << __func__ << ": Checking hash of " << i->first << dendl;
          bufferhash h(-1);
          h << bl;
          if (h.digest() != hinfo->get_chunk_hash(shard)) {
            get_parent()->clog_error() << "Bad hash for " << i->first << " digest 0x"
                                       << hex << h.digest() << " expected 0x" << hinfo->get_chunk_hash(shard) << dec;
            dout(5) << __func__ << ": Bad hash for " << i->first << " digest 0x"
                    << hex << h.digest() << " expected 0x" << hinfo->get_chunk_hash(shard) << dec << dendl;
            r = -EIO;
            return true;
          }
        }
      } // endof hash check
    } // endof to_read iteration
  } // endof success in reading
  return false;
}

template <typename MapIterator>
bool ECBackend::handle_trivial_sub_read(
  pg_shard_t from,
  const ECSubRead &op,
  ECSubReadReply *reply,
  const ZTracer::Trace &trace,
  MapIterator &i,
  int &r,
  shard_id_t &shard,
  ECUtil::HashInfoRef &hinfo)
{
  for (auto j = i->second.begin(); j != i->second.end(); ++j) {
    bufferlist bl;
    r = store->read(
      ch,
      ghobject_t(i->first, ghobject_t::NO_GEN, shard),
      j->template get<0>(),
      j->template get<1>(),
      bl, j->template get<2>());
    if (r < 0) {
      get_parent()->clog_error() << "Error " << r
                                 << " reading object "
                                 << i->first;
      dout(5) << __func__ << ": Error " << r
              <<" reading " << i->first << dendl;
      return true;
    } else {
      dout(20) << __func__ << " read request=" << j->template get<1>() << " r=" << r << " len=" << bl.length() << dendl;
      reply->buffers_read[i->first].push_back(
        make_pair(
          j->template get<0>(),
          bl)
        );
    }

    if (!get_parent()->get_pool().allows_ecoverwrites()) {
      // This shows that we still need deep scrub because large enough files
      // are read in sections, so the digest check here won't be done here.
      // Do NOT check osd_read_eio_on_bad_digest here.  We need to report
      // the state of our chunk in case other chunks could substitute.
      assert(hinfo->has_chunk_hash());
      if ((bl.length() == hinfo->get_total_chunk_size()) &&
          (j->template get<0>() == 0)) {
        dout(20) << __func__ << ": Checking hash of " << i->first << dendl;
        bufferhash h(-1);
        h << bl;
        if (h.digest() != hinfo->get_chunk_hash(shard)) {
          get_parent()->clog_error() << "Bad hash for " << i->first << " digest 0x"
                                     << hex << h.digest() << " expected 0x"
                                     << hinfo->get_chunk_hash(shard) << dec;
          dout(5) << __func__ << ": Bad hash for " << i->first << " digest 0x"
                  << hex << h.digest() << " expected 0x" << hinfo->get_chunk_hash(shard)
                  << dec << dendl;
          r = -EIO;
          return true;
        }
      }
    }
  }
  return false;
}

template <typename MapIterator>
bool ECBackend::handle_conservative_sub_read(
  pg_shard_t from,
  const ECSubRead &op,
  ECSubReadReply *reply,
  const ZTracer::Trace &trace,
  MapIterator &i,
  int &r,
  shard_id_t &shard,
  ECUtil::HashInfoRef &hinfo)
{
  // If list is not empty:
  if (i->second.begin() == i->second.end()) {
    return false;
  }
  // Iterate over element ids in elements_map's kth column
  // otherwise not yet implemented
  assert(ECBackend::net_type == ReadType::Trivial);
  uint64_t element_size = sinfo.get_chunk_size() / sinfo.get_row_count();
  uint64_t subchunk_start = i->second.front().template get<0>();
  uint64_t subchunk_len = i->second.front().template get<1>();
  dout(1) << "MatanLiramV10: element_size: " << element_size << " subchunk_start: "
    << subchunk_start << " subchunk_len: " << subchunk_len << dendl;
  // We assume implicitly that elements are sorted ascendingly
  for (auto j = i->second.begin(); j != i->second.end(); ++j) {
    bufferlist bl;
    // If this read is sequential, skip to the last chunk in the continuous part
    if (std::next(j) != i->second.end()
        && std::next(j)->template get<0>() == \
          j->template get<0>() + j->template get<1>()) {
      // skip this read
      assert(j->template get<1>() == element_size); // assert trivial network
      subchunk_len += j->template get<1>();
      continue;
    }
    if (j->template get<1>() > 0) { // or subchunk_len > 0 if there's no extreme case
      dout(1) << "MatanLiramV10: conservatively read: [" << subchunk_start << ","
        << subchunk_len << "]" << dendl;
      r = store->read(
          ch, // ObjectStore::CollectionHandle
          ghobject_t(i->first, ghobject_t::NO_GEN, shard),
          subchunk_start,
          subchunk_len,
          bl, j->template get<2>());
    } else { // If read is empty, don't ask objectstore to read
      r = 0;
    }
    if (r < 0) {
      get_parent()->clog_error() << __func__
        << ": Error " << r
        << " reading "
        << i->first;
      dout(5) << __func__ << ": Error " << r
        << " reading " << i->first << dendl;
      return true;
    } else { // read succeeded
      dout(20) << __func__ << " read request=" << j->template get<1>() << " r=" << r
        << " len=" << bl.length() << dendl;
      // Make sure that we know how to read buffers_read later
      for (unsigned k = 0; k < subchunk_len; k += element_size) {
        bufferlist element_bl;
        // from element_offset to beginning of next element
        element_bl.substr_of(bl, k, element_size);
        reply->buffers_read[i->first].push_back(
            make_pair(
              subchunk_start + k,
              element_bl)
            );
      }
    }
    
    // MatanLiramV6: gives -EIO, check why hash is bad after our changes...
    // This shows that we still need deep scrub because large enough files
    // are read in sections, so the digest check here won't be done here.
    // Do NOT check osd_read_eio_on_bad_digest here.  We need to report
    // the state of our chunk in case other chunks could substitute.
    if ((bl.length() == hinfo->get_total_chunk_size()) &&
        (subchunk_start == 0)) { //j->get<0>() == 0)) {
      dout(20) << __func__ << ": Checking hash of " << i->first << dendl;
      bufferhash h(-1);
      h << bl;
      if (h.digest() != hinfo->get_chunk_hash(shard)) {
        get_parent()->clog_error() << __func__ << ": Bad hash for " << i->first
          << " digest 0x" << hex << h.digest() << " expected 0x"
          << hinfo->get_chunk_hash(shard) << dec << "\n";
        dout(5) << __func__ << ": Bad hash for " << i->first << " digest 0x"
          << hex << h.digest() << " expected 0x" << hinfo->get_chunk_hash(shard)
          << dec << dendl;
        r = -EIO;
        return true;
      }
    }

    if (std::next(j) != i->second.end()) {
      subchunk_len = element_size;
      subchunk_start = std::next(j)->template get<0>();
    }
  } // endof to_read iteration
  return false;
}

void ECBackend::handle_sub_read(
  pg_shard_t from,
  const ECSubRead &op,
  ECSubReadReply *reply,
  const ZTracer::Trace &trace)
{
  bool is_error = false;
  trace.event("handle sub read");
  shard_id_t shard = get_parent()->whoami_shard().shard;
  for(auto i = op.to_read.begin();
      i != op.to_read.end();
      ++i) {
    int r = 0;
    ECUtil::HashInfoRef hinfo;
    if (!get_parent()->get_pool().allows_ecoverwrites()) {
      hinfo = get_hash_info(i->first);
      if (!hinfo) {
	r = -EIO;
	get_parent()->clog_error() << "Corruption detected: object " << i->first
                                   << " is missing hash_info";
	dout(5) << __func__ << ": No hinfo for " << i->first << dendl;
	goto error;
      }
    }
    // Aggressive reads are implemented as follows: we get
    // conservative in the buffer from start_read_op(), take the first and last
    // positions and read between them. Then naming the first position firstoff,
    // in order to read x,len we copy x-firstoff,len from the given buffer.
    switch (read_type)
    {
    case ReadType::Aggressive:
      is_error = handle_aggressive_sub_read(from, op, reply, trace, i, r, shard, hinfo);
      break;
    case ReadType::Trivial:
      is_error = handle_trivial_sub_read(from, op, reply, trace, i, r, shard, hinfo);
      break;
    case ReadType::Conservative:
      is_error = handle_conservative_sub_read(from, op, reply, trace, i, r, shard,
          hinfo);
      break;
    default:
      dout(1) << __func__ << "Invalid read type." << dendl;
      goto error;
      break;
    }
    if (is_error) {
      goto error;
    }
    continue;
error:
    // Do NOT check osd_read_eio_on_bad_digest here.  We need to report
    // the state of our chunk in case other chunks could substitute.
    reply->buffers_read.erase(i->first);
    reply->errors[i->first] = r;
  }
  for (set<hobject_t>::iterator i = op.attrs_to_read.begin();
       i != op.attrs_to_read.end();
       ++i) {
    dout(10) << __func__ << ": fulfilling attr request on "
	     << *i << dendl;
    if (reply->errors.count(*i))
      continue;
    int r = store->getattrs(
      ch,
      ghobject_t(
	*i, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      reply->attrs_read[*i]);
    if (r < 0) {
      reply->buffers_read.erase(*i);
      reply->errors[*i] = r;
    }
  }
  reply->from = get_parent()->whoami_shard();
  reply->tid = op.tid;
}

void ECBackend::handle_sub_write_reply(
  pg_shard_t from,
  const ECSubWriteReply &op,
  const ZTracer::Trace &trace)
{
  map<ceph_tid_t, Op>::iterator i = tid_to_op_map.find(op.tid);
  assert(i != tid_to_op_map.end());
  if (op.committed) {
    trace.event("sub write committed");
    assert(i->second.pending_commit.count(from));
    i->second.pending_commit.erase(from);
    if (from != get_parent()->whoami_shard()) {
      get_parent()->update_peer_last_complete_ondisk(from, op.last_complete);
    }
  }
  if (op.applied) {
    trace.event("sub write applied");
    assert(i->second.pending_apply.count(from));
    i->second.pending_apply.erase(from);
  }

  if (i->second.pending_apply.empty() && i->second.on_all_applied) {
    dout(10) << __func__ << " Calling on_all_applied on " << i->second << dendl;
    i->second.on_all_applied->complete(0);
    i->second.on_all_applied = 0;
    i->second.trace.event("ec write all applied");
  }
  if (i->second.pending_commit.empty() && i->second.on_all_commit) {
    dout(10) << __func__ << " Calling on_all_commit on " << i->second << dendl;
    i->second.on_all_commit->complete(0);
    i->second.on_all_commit = 0;
    i->second.trace.event("ec write all committed");
  }
  check_ops();
}

void ECBackend::handle_sub_read_reply(
  pg_shard_t from,
  ECSubReadReply &op,
  RecoveryMessages *m,
  const ZTracer::Trace &trace)
{
  trace.event("ec sub read reply");
  dout(10) << __func__ << ": reply " << op << dendl;
  // tid stands for transaction id and it's a uint64_t.
  map<ceph_tid_t, ReadOp>::iterator iter = tid_to_read_map.find(op.tid);
  if (iter == tid_to_read_map.end()) {
    //canceled
    dout(20) << __func__ << ": dropped " << op << dendl;
    return;
  }
  // Needs it to progress buffers_read to next chunk
  uint64_t chunk_size = sinfo.get_chunk_size();
  // For renewing asserts
  uint64_t element_size = chunk_size / sinfo.get_row_count();
  dout(1) << "MatanLiramV5: chunk size in handle_sub_read_reply is: " <<
    chunk_size << dendl;
  ReadOp &rop = iter->second;
  // buffers_read have start_offset, contents buffer pair, constructed
  // in handle_sub_read.
  for (auto i = op.buffers_read.begin();
       i != op.buffers_read.end();
       ++i) {
    dout(1) << __func__ << "buffers_read list is of size: " << i->second.size()
      << dendl;
    // If attribute error we better not have sent a buffer
    assert(!op.errors.count(i->first));
    if (!rop.to_read.count(i->first)) {
      // We canceled this read! @see filter_read_op
      dout(20) << __func__ << " to_read skipping" << dendl;
      continue;
    }
    // Get the to_read of read_request_t obj
    // rop.to_red.find(..)->second is read_request_t.
    // We are iterating over its to_read.
    list<boost::tuple<uint64_t, uint64_t, uint32_t> >::const_iterator req_iter =
      rop.to_read.find(i->first)->second.to_read.begin();
    // Pass elements_map to read_result_t:
    dout(1) << __func__ << "complete has " << rop.complete.size()
      << "elements." << dendl;
    const map<int, set<int> > &elements_map = rop.to_read.find(
        i->first)->second.elements_map;
    dout(1) << __func__ << "elements_map size is: " << elements_map.size()
      << dendl;

    // Notice tht rop.complete needs the read_result_t to be updated once,
    // while this function is called in each of the local OSDs of the PG.
    // Therefore we update it only if its elements_map is not initialized.
    if (elements_map.size() > 0 &&
          (rop.complete.count(i->first) == 0 ||
           rop.complete.find(i->first)->second.elements_map.size() == 0)) {
      // rop.to_read is a map from hobject_t to read_request_t, so we get the
      // elements map from read_request_t and pass it to read_result_t.
      //
      // What happened here is that std::map::operator[] calls empty c'tor,
      // which makes it impossible to initialize read_result_t's
      // const elements_map&. Therefore we used the "insert" method instead,
      // but we also want to make sure the key was not initialized before
      // because it doesn't make sense that an hobject_t key will have 2 different
      // read_result_t values.
      dout(1) << "MatanLiramV6: Initializing elements_map in read_result_t"
        << dendl;
      // apparently insert cannot override elements, we have no elements_map in
      // result... -.-
      // rop.complete.insert(map<hobject_t, read_result_t>::value_type(
      //       i->first, read_result_t(elements_map)));
      // MatanLiramV7: fix elements_map to be non-const, then we can update it
      // using operator[].
      rop.complete[i->first].elements_map = elements_map;
    }

    list<
      boost::tuple<
	uint64_t, uint64_t, map<pg_shard_t, bufferlist> > >::iterator riter =
      rop.complete[i->first].returned.begin();
    // Each read request (req_iter) corresponds to a full chunk read while
    // each buffers_read element (j) corresponds to a single element read.
    // We can concatenate all j reads of the same chunk into the same bufferlist
    // in riter, and then add zeroes in decode (or use a special decode).
    // riter is initialized by full chunk reads in start_read_op.
    /*for (list<pair<uint64_t, bufferlist> >::iterator j = i->second.begin();
	 j != i->second.end();
	 ++j, ++req_iter, ++riter) {
      assert(req_iter != rop.to_read.find(i->first)->second.to_read.end());
      assert(riter != rop.complete[i->first].returned.end());
      pair<uint64_t, uint64_t> adjusted =
	sinfo.aligned_offset_len_to_chunk(
	  make_pair(req_iter->get<0>(), req_iter->get<1>()));
      assert(adjusted.first == j->first);
      riter->get<2>()[from].claim(j->second);
    }*/
    for (list<pair<uint64_t, bufferlist> >::iterator j = i->second.begin();
        j != i->second.end();
        ++req_iter, ++riter) {
      // MatanLiramDoc: asserts req_iter didn't get to end while still
      // iterating on j
      assert(req_iter != rop.to_read.find(i->first)->second.to_read.end());
      // same thing with result_iter,
      assert(riter != rop.complete[i->first].returned.end());
      // Chunk aligned whereas our reads are element aligned.
      pair<uint64_t, uint64_t> adjusted =
        sinfo.aligned_offset_len_to_chunk(
            make_pair(req_iter->get<0>(), req_iter->get<1>()));
      // If we advance j to the next chunk each time, this should be correct
      dout(1) << "adjusted.first: " << adjusted.first << " and j->first: "
        << j->first << dendl;
      // Could be that read starts from not-zero element:
      assert(j->first % element_size == 0);
      assert(adjusted.first <= j->first && adjusted.first + chunk_size > j->first);
      // Claim is "sort-of-like-assignment-op" (according to buffer.cc)
      // Need to separate appended ops before we hand them to Ido,
      // maybe in reconstruct_concat, or maybe before (makes a bit more sense to
      // do it before because it's implementation dependent and not related to
      // the plugin).

      // Inserting chunk sent from OSD "from" to read_result_t.
      // Iterate over j until we're at the next chunk, add read buffers from
      // chunk to returned bufferlist in riter.
      riter->get<2>()[from].clear(); // empty buffer
      uint64_t recovery_chunk = get_recovery_chunk_size() / sinfo.get_k();
      dout(1) << __func__ << "recovery chunk size is: " << recovery_chunk
        << dendl;
      for (; j != i->second.end() && j->first < adjusted.first + recovery_chunk;
          ++j) {
        // Should work also when bl is empty in case of an empty
        // shard read when recovering a coding chunk.
        dout(1) << __func__ << "read starts in " << j->first << dendl;
        riter->get<2>()[from].claim_append(j->second);
      }
    }
  }

  // Searched for assertion errors here:
  for (auto i = op.attrs_read.begin();
       i != op.attrs_read.end();
       ++i) {
    // if read error better not have sent an attribute
    assert(!op.errors.count(i->first));
    if (!rop.to_read.count(i->first)) {
      // We canceled this read! @see filter_read_op
      dout(20) << __func__ << " to_read skipping" << dendl;
      continue;
    }
    rop.complete[i->first].attrs = map<string, bufferlist>();
    (*(rop.complete[i->first].attrs)).swap(i->second);
  }
  for (auto i = op.errors.begin();
       i != op.errors.end();
       ++i) {
    rop.complete[i->first].errors.insert(
      make_pair(
	from,
	i->second));
    dout(20) << __func__ << " shard=" << from << " error=" << i->second << dendl;
  }

  map<pg_shard_t, set<ceph_tid_t> >::iterator siter =
					shard_to_read_map.find(from);
  assert(siter != shard_to_read_map.end());
  assert(siter->second.count(op.tid));
  siter->second.erase(op.tid);

  assert(rop.in_progress.count(from));
  rop.in_progress.erase(from);
  unsigned is_complete = 0;
  // For redundant reads check for completion as each shard comes in,
  // or in a non-recovery read check for completion once all the shards read.
  // TODO: It would be nice if recovery could send more reads too
  if (rop.do_redundant_reads || (!rop.for_recovery && rop.in_progress.empty())) {
    for (map<hobject_t, read_result_t>::const_iterator iter =
        rop.complete.begin();
      iter != rop.complete.end();
      ++iter) {
      set<int> have;
      for (map<pg_shard_t, bufferlist>::const_iterator j =
          iter->second.returned.front().get<2>().begin();
        j != iter->second.returned.front().get<2>().end();
        ++j) {
        have.insert(j->first.shard);
        dout(20) << __func__ << " have shard=" << j->first.shard << dendl;
      }
      set<int> want_to_read, dummy_minimum;
      // Get shard ids corresponding to chunks  0 to k-1.
      get_want_to_read_shards(&want_to_read);
      int err;
      // Changed interface to required_to_reconstruct
      // required_to_reconstruct returns -EINVAL only if #erasures > 1.
      // It means we'll never enter this sequence in our experiments
      dout(1) << __func__ << "required_to_reconstruct Flow 1" << dendl;
      if ((err = ec_impl->required_to_reconstruct(want_to_read, have,
              &dummy_minimum, static_cast<map<int, set<int> >*>(0))) < 0) {
        dout(20) << __func__ << "required_to_reconstruct failed" << dendl;
        if (rop.in_progress.empty()) {
	  // If we don't have enough copies and we haven't sent reads for all shards
	  // we can send the rest of the reads, if any.
	  if (!rop.do_redundant_reads) {
	    int r = send_all_remaining_reads(iter->first, rop);
	    if (r == 0) {
	      // We added to in_progress and not incrementing is_complete
	      continue;
	    }
	    // Couldn't read any additional shards so handle as completed with errors
	  }
	  // We don't want to confuse clients / RBD with objectstore error
	  // values in particular ENOENT.  We may have different error returns
	  // from different shards, so we'll return minimum_to_decode() error
	  // (usually EIO) to reader.  It is likely an error here is due to a
	  // damaged pg.
	  rop.complete[iter->first].r = err;
	  ++is_complete;
	}
      } else { // required_to_reconstruct succeeded
        assert(rop.complete[iter->first].r == 0);
	if (!rop.complete[iter->first].errors.empty()) {
	  if (cct->_conf->osd_read_ec_check_for_errors) {
	    dout(10) << __func__ << ": Not ignoring errors, use one shard err=" << err << dendl;
	    err = rop.complete[iter->first].errors.begin()->second;
            rop.complete[iter->first].r = err;
	  } else {
	    get_parent()->clog_warn() << "Error(s) ignored for "
				       << iter->first << " enough copies available";
	    dout(10) << __func__ << " Error(s) ignored for " << iter->first
		     << " enough copies available" << dendl;
	    rop.complete[iter->first].errors.clear();
	  }
	}
	++is_complete;
      }
    }
  }
  if (rop.in_progress.empty() || is_complete == rop.complete.size()) {
    dout(20) << __func__ << " Complete: " << rop << dendl;
    rop.trace.event("ec read complete");
    complete_read_op(rop, m);
  } else {
    dout(10) << __func__ << " readop not complete: " << rop << dendl;
  }
}

void ECBackend::complete_read_op(ReadOp &rop, RecoveryMessages *m)
{
  map<hobject_t, read_request_t>::iterator reqiter =
    rop.to_read.begin();
  map<hobject_t, read_result_t>::iterator resiter =
    rop.complete.begin();
  // Num of objects requested to read are all read.
  assert(rop.to_read.size() == rop.complete.size());
  for (; reqiter != rop.to_read.end(); ++reqiter, ++resiter) {
    if (reqiter->second.cb) { // if request has a callback, call it (i.e. decode)
      dout(1) << __func__ << "elements map is of len "
        << resiter->second.elements_map.size() << dendl;
      pair<RecoveryMessages *, read_result_t &> arg(
	m, resiter->second);
      // In case of a parity element read fault (hash probably), that's how
      // we get informed about it:
      if (!resiter->second.errors.empty()) {
        dout(0) << "MatanLiramV8: assert empty has failed: " << resiter->second.errors
          << dendl;
      }
      reqiter->second.cb->complete(arg);
      reqiter->second.cb = NULL;
    }
  }
  tid_to_read_map.erase(rop.tid); // erase transaction
}

struct FinishReadOp : public GenContext<ThreadPool::TPHandle&>  {
  ECBackend *ec;
  ceph_tid_t tid;
  FinishReadOp(ECBackend *ec, ceph_tid_t tid) : ec(ec), tid(tid) {}
  void finish(ThreadPool::TPHandle &handle) override {
    auto ropiter = ec->tid_to_read_map.find(tid);
    assert(ropiter != ec->tid_to_read_map.end());
    int priority = ropiter->second.priority;
    RecoveryMessages rm;
    ec->complete_read_op(ropiter->second, &rm);
    ec->dispatch_recovery_messages(rm, priority);
  }
};

void ECBackend::filter_read_op(
  const OSDMapRef& osdmap,
  ReadOp &op)
{
  set<hobject_t> to_cancel;
  for (map<pg_shard_t, set<hobject_t> >::iterator i = op.source_to_obj.begin();
       i != op.source_to_obj.end();
       ++i) {
    if (osdmap->is_down(i->first.osd)) {
      to_cancel.insert(i->second.begin(), i->second.end());
      op.in_progress.erase(i->first);
      continue;
    }
  }

  if (to_cancel.empty())
    return;

  for (map<pg_shard_t, set<hobject_t> >::iterator i = op.source_to_obj.begin();
       i != op.source_to_obj.end();
       ) {
    for (set<hobject_t>::iterator j = i->second.begin();
	 j != i->second.end();
	 ) {
      if (to_cancel.count(*j))
	i->second.erase(j++);
      else
	++j;
    }
    if (i->second.empty()) {
      op.source_to_obj.erase(i++);
    } else {
      assert(!osdmap->is_down(i->first.osd));
      ++i;
    }
  }

  for (set<hobject_t>::iterator i = to_cancel.begin();
       i != to_cancel.end();
       ++i) {
    get_parent()->cancel_pull(*i);

    assert(op.to_read.count(*i));
    read_request_t &req = op.to_read.find(*i)->second;
    dout(10) << __func__ << ": canceling " << req
	     << "  for obj " << *i << dendl;
    assert(req.cb);
    delete req.cb;
    req.cb = NULL;

    op.to_read.erase(*i);
    op.complete.erase(*i);
    recovery_ops.erase(*i);
  }

  if (op.in_progress.empty()) {
    get_parent()->schedule_recovery_work(
      get_parent()->bless_gencontext(
	new FinishReadOp(this, op.tid)));
  }
}

void ECBackend::check_recovery_sources(const OSDMapRef& osdmap)
{
  set<ceph_tid_t> tids_to_filter;
  for (map<pg_shard_t, set<ceph_tid_t> >::iterator 
       i = shard_to_read_map.begin();
       i != shard_to_read_map.end();
       ) {
    if (osdmap->is_down(i->first.osd)) {
      tids_to_filter.insert(i->second.begin(), i->second.end());
      shard_to_read_map.erase(i++);
    } else {
      ++i;
    }
  }
  for (set<ceph_tid_t>::iterator i = tids_to_filter.begin();
       i != tids_to_filter.end();
       ++i) {
    map<ceph_tid_t, ReadOp>::iterator j = tid_to_read_map.find(*i);
    assert(j != tid_to_read_map.end());
    filter_read_op(osdmap, j->second);
  }
}

void ECBackend::on_change()
{
  dout(10) << __func__ << dendl;

  completed_to = eversion_t();
  committed_to = eversion_t();
  pipeline_state.clear();
  waiting_reads.clear();
  waiting_state.clear();
  waiting_commit.clear();
  for (auto &&op: tid_to_op_map) {
    cache.release_write_pin(op.second.pin);
  }
  tid_to_op_map.clear();

  for (map<ceph_tid_t, ReadOp>::iterator i = tid_to_read_map.begin();
       i != tid_to_read_map.end();
       ++i) {
    dout(10) << __func__ << ": cancelling " << i->second << dendl;
    for (map<hobject_t, read_request_t>::iterator j =
	   i->second.to_read.begin();
	 j != i->second.to_read.end();
	 ++j) {
      delete j->second.cb;
      j->second.cb = 0;
    }
  }
  tid_to_read_map.clear();
  in_progress_client_reads.clear();
  shard_to_read_map.clear();
  clear_recovery_state();
}

void ECBackend::clear_recovery_state()
{
  recovery_ops.clear();
}

void ECBackend::on_flushed()
{
}

void ECBackend::dump_recovery_info(Formatter *f) const
{
  f->open_array_section("recovery_ops");
  for (map<hobject_t, RecoveryOp>::const_iterator i = recovery_ops.begin();
       i != recovery_ops.end();
       ++i) {
    f->open_object_section("op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("read_ops");
  for (map<ceph_tid_t, ReadOp>::const_iterator i = tid_to_read_map.begin();
       i != tid_to_read_map.end();
       ++i) {
    f->open_object_section("read_op");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void ECBackend::submit_transaction(
  const hobject_t &hoid,
  const object_stat_sum_t &delta_stats,
  const eversion_t &at_version,
  PGTransactionUPtr &&t,
  const eversion_t &trim_to,
  const eversion_t &roll_forward_to,
  const vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_history,
  Context *on_local_applied_sync,
  Context *on_all_applied,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef client_op
  )
{
  assert(!tid_to_op_map.count(tid));
  Op *op = &(tid_to_op_map[tid]);
  op->hoid = hoid;
  op->delta_stats = delta_stats;
  op->version = at_version;
  op->trim_to = trim_to;
  op->roll_forward_to = MAX(roll_forward_to, committed_to);
  op->log_entries = log_entries;
  std::swap(op->updated_hit_set_history, hset_history);
  op->on_local_applied_sync = on_local_applied_sync;
  op->on_all_applied = on_all_applied;
  op->on_all_commit = on_all_commit;
  op->tid = tid;
  op->reqid = reqid;
  op->client_op = client_op;
  if (client_op)
    op->trace = client_op->pg_trace;
  
  dout(10) << __func__ << ": op " << *op << " starting" << dendl;
  start_rmw(op, std::move(t));
  dout(10) << "onreadable_sync: " << op->on_local_applied_sync << dendl;
}

void ECBackend::call_write_ordered(std::function<void(void)> &&cb) {
  if (!waiting_state.empty()) {
    waiting_state.back().on_write.emplace_back(std::move(cb));
  } else if (!waiting_reads.empty()) {
    waiting_reads.back().on_write.emplace_back(std::move(cb));
  } else {
    // Nothing earlier in the pipeline, just call it
    cb();
  }
}

// Added elements_map which maps to_read shards to the elements read from them.
int ECBackend::get_min_avail_to_read_shards(
  const hobject_t &hoid,
  const set<int> &want,
  bool for_recovery,
  bool do_redundant_reads,
  set<pg_shard_t> *to_read,
  map<int, set<int> > &elements_map)
{
  // Make sure we don't do redundant reads for recovery
  assert(!for_recovery || !do_redundant_reads);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;

  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_shards().begin();
       i != get_parent()->get_acting_shards().end();
       ++i) {
    dout(10) << __func__ << ": checking acting " << *i << dendl;
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    // Check if shard is missing in the object's placement group
    if (!missing.is_missing(hoid)) {
      assert(!have.count(i->shard));
      have.insert(i->shard);
      assert(!shards.count(i->shard));
      shards.insert(make_pair(i->shard, *i));
    }
  }

  if (for_recovery) {
    for (set<pg_shard_t>::const_iterator i =
	   get_parent()->get_backfill_shards().begin();
	 i != get_parent()->get_backfill_shards().end();
	 ++i) {
      if (have.count(i->shard)) {
	assert(shards.count(i->shard));
	continue;
      }
      dout(10) << __func__ << ": checking backfill " << *i << dendl;
      assert(!shards.count(i->shard));
      const pg_info_t &info = get_parent()->get_shard_info(*i);
      const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
      if (hoid < info.last_backfill &&
	  !missing.is_missing(hoid)) {
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }

    map<hobject_t, set<pg_shard_t>>::const_iterator miter =
      get_parent()->get_missing_loc_shards().find(hoid);
    if (miter != get_parent()->get_missing_loc_shards().end()) {
      for (set<pg_shard_t>::iterator i = miter->second.begin();
	   i != miter->second.end();
	   ++i) {
	dout(10) << __func__ << ": checking missing_loc " << *i << dendl;
	auto m = get_parent()->maybe_get_shard_missing(*i);
	if (m) {
	  assert(!(*m).is_missing(hoid));
	}
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }
  }

  set<int> need;
  int r = ec_impl->required_to_reconstruct(want, have, &need, &elements_map);
  if (r < 0)
    return r;

  if (do_redundant_reads) {
      need.swap(have);
  } 

  if (!to_read)
    return 0;

  for (set<int>::iterator i = need.begin();
       i != need.end();
       ++i) {
    assert(shards.count(shard_id_t(*i)));
    to_read->insert(shards[shard_id_t(*i)]);
  }
  return 0;
}

int ECBackend::get_remaining_shards(
  const hobject_t &hoid,
  const set<int> &avail,
  set<pg_shard_t> *to_read)
{
  set<int> need;
  map<shard_id_t, pg_shard_t> shards;

  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_shards().begin();
       i != get_parent()->get_acting_shards().end();
       ++i) {
    dout(10) << __func__ << ": checking acting " << *i << dendl;
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    if (!missing.is_missing(hoid)) {
      assert(!need.count(i->shard));
      need.insert(i->shard);
      assert(!shards.count(i->shard));
      shards.insert(make_pair(i->shard, *i));
    }
  }

  if (!to_read)
    return 0;

  for (set<int>::iterator i = need.begin();
       i != need.end();
       ++i) {
    assert(shards.count(shard_id_t(*i)));
    if (avail.find(*i) == avail.end())
      to_read->insert(shards[shard_id_t(*i)]);
  }
  return 0;
}

void ECBackend::start_read_op(
  int priority,
  map<hobject_t, read_request_t> &to_read,
  OpRequestRef _op,
  bool do_redundant_reads,
  bool for_recovery)
{
  // Still original to_read, didn't cut eements map off it yet
  for (map<hobject_t, read_request_t>::iterator i = to_read.begin();
      i != to_read.end(); ++i) {
    dout(1) << "MatanLiramV6: start_read_op, elements_map of object " << i->first
      << "is of size " << i->second.elements_map.size() << dendl;
  }
  ceph_tid_t tid = get_parent()->get_tid();
  assert(!tid_to_read_map.count(tid));
  auto &op = tid_to_read_map.emplace(
    tid,
    ReadOp(
      priority,
      tid,
      do_redundant_reads,
      for_recovery,
      _op,
      std::move(to_read))).first->second;
  dout(10) << __func__ << ": starting " << op << dendl;
  if (_op) {
    op.trace = _op->pg_trace;
    op.trace.event("start ec read");
  }
  do_read_op(op);
}

void ECBackend::insert_trivial_chunk_to_messages(
    pair<uint64_t, uint64_t> &chunk_off_len,
    uint64_t &off,
    map<pg_shard_t, ECSubRead> &messages,
    map<hobject_t, read_request_t>::iterator i,
    list<boost::tuple<uint64_t, uint64_t, uint32_t> >::const_iterator &j,
    set<pg_shard_t>::const_iterator &k,
    const set<int> &elements,
    const uint64_t &element_size)
{
  // Iterate over element ids in elements_map's kth
  // column
  for (set<int>::const_iterator el = elements.begin();
      el != elements.end(); ++el) {
    // insert the to_read of this object a tuple with offset,
    // length, flags
    dout(1) << __func__ << "adding " << chunk_off_len.first << "-"
        << off << "-" << (*el * element_size) << " read of length "
        << element_size << dendl;
    messages[*k].to_read[i->first].push_back(boost::make_tuple(
          chunk_off_len.first + off + (*el * element_size),
          element_size,
          j->get<2>()));
    // This read will be concatenated in handle_sub_read_reply
    // where we changed the ++j
  }
}

void ECBackend::insert_conservative_chunk_to_messages(
    pair<uint64_t, uint64_t> &chunk_off_len,
    uint64_t &off,
    map<pg_shard_t, ECSubRead> &messages,
    map<hobject_t, read_request_t>::iterator &i,
    list<boost::tuple<uint64_t, uint64_t, uint32_t> >::const_iterator &j,
    set<pg_shard_t>::const_iterator &k,
    const set<int> &elements,
    const uint64_t &element_size)
{
  // Iterate over element ids in elements_map's kth
  // column
  int subchunk_start = (elements.size() > 0) ? (*elements.begin())  : 0;
  uint64_t subchunk_len = 1;
  // We assume implicitly that elements are sorted ascendingly
  for (set<int>::const_iterator el = elements.begin();
      el != elements.end(); ++el) {
    // If this read is continued, skip to the end of it
    if (std::next(el) != elements.end()
        && *std::next(el) == *el + 1) {
      // skip this read
      subchunk_len++;
      continue;
    }
    // insert the to_read of this object a tuple with offset,
    // length, flags
    dout(1) << __func__ << "adding " << chunk_off_len.first
        << "-" << off << "-" << (subchunk_start * element_size)
        << " read of length " << subchunk_len*element_size << dendl;
    messages[*k].to_read[i->first].push_back(boost::make_tuple(
          chunk_off_len.first + off
              + (subchunk_start*element_size),
          subchunk_len*element_size,
          j->get<2>()));
    subchunk_len = 1;
    subchunk_start = *el;
    // This read will be concatenated in handle_sub_read_reply
    // where we changed the ++j
  }
}

void ECBackend::do_read_op(ReadOp &op)
{
  int priority = op.priority;
  ceph_tid_t tid = op.tid;

  dout(10) << __func__ << ": starting read " << op << dendl;

  const uint64_t chunk_size = sinfo.get_chunk_size();
  dout(1) << "MatanLiram start_read_op chunk_size: " << chunk_size << dendl;
  const uint64_t element_size = chunk_size / sinfo.get_row_count();
  dout(1) << "MatanLiram start_read_op element_size: " << element_size << dendl;
  map<pg_shard_t, ECSubRead> messages;
  for (map<hobject_t, read_request_t>::iterator i = op.to_read.begin();
       i != op.to_read.end();
       ++i) {
    bool need_attrs = i->second.want_attrs;
    for (set<pg_shard_t>::const_iterator j = i->second.need.begin();
	 j != i->second.need.end();
	 ++j) {
      if (need_attrs) {
	messages[*j].attrs_to_read.insert(i->first);
	need_attrs = false;
      }
      op.obj_to_source[i->first].insert(*j);
      op.source_to_obj[*j].insert(i->first);
    }
    // Get elements map from read request
    // inserted change after changed read_request_t
    const map<int, set<int> > &elements_map = i->second.elements_map;
    dout(1) << __func__ << "got elements map" << dendl;
    // Initialize by inserting full chunk reads to the complete[].returned array
    for (list<boost::tuple<uint64_t, uint64_t, uint32_t> >::const_iterator j =
	   i->second.to_read.begin();
	 j != i->second.to_read.end();
	 ++j) {
      pair<uint64_t, uint64_t> chunk_off_len =
	sinfo.aligned_offset_len_to_chunk(make_pair(j->get<0>(), j->get<1>()));
      for (set<pg_shard_t>::const_iterator k = i->second.need.begin();
	   k != i->second.need.end();
	   ++k) {
        if (elements_map.size() == 0) {
          messages[*k].to_read[i->first].push_back(
            boost::make_tuple(
              chunk_off_len.first,
              chunk_off_len.second,
              j->get<2>()));
        }
        else
        {
          // Zero sized sharded reads are possible when we recover a parity
          // node by reconstructing all of the data nodes
          if (elements_map.find(k->shard.id) == elements_map.end()) {
            // insert an empty read so that decode can extract the erasure in case
            // of a parity recovery of the other coding node in zigzag
            messages[*k].to_read[i->first].push_back(boost::make_tuple(
                  chunk_off_len.first, 0, j->get<2>()));
            continue;
          }
          dout(1) << __func__ << "inside non-parity shard" << dendl;
          const set<int> &elements = elements_map.find(k->shard.id)->second;
          dout(1) << __func__ << "elements: " << elements << "("
            << k->shard.id << ")" << dendl;
          // MatanLiramDoc: iterate on full chunks in this outer loop
          for (uint64_t off=0; off < chunk_off_len.second; off += chunk_size) {
            // Default net_type is Trivial since it doesn't put mumch overhead
            switch (ECBackend::net_type) {
            case ReadType::Trivial:
              insert_trivial_chunk_to_messages(chunk_off_len, off, messages,
                  i, j, k, elements, element_size);
              break;
            case ReadType::Conservative:
              insert_conservative_chunk_to_messages(chunk_off_len, off,
                  messages, i, j, k, elements, element_size);
              break;
            case ReadType::Aggressive:
              dout(0) << __func__ << "Aggressive network can't be implemented "
                << "(loss of information)" << dendl;
              assert(false);
              break;
            }
          }
        }
      }
      assert(!need_attrs);
    }
  }

  // Prepare messages of ECSubRead, send to messages[i]->first.osd meaning
  // it sends the message to the shard's OSD.
  for (map<pg_shard_t, ECSubRead>::iterator i = messages.begin();
       i != messages.end();
       ++i) {
    op.in_progress.insert(i->first);
    shard_to_read_map[i->first].insert(op.tid);
    i->second.tid = tid;
    MOSDECSubOpRead *msg = new MOSDECSubOpRead;
    msg->set_priority(priority);
    msg->pgid = spg_t(
      get_parent()->whoami_spg_t().pgid,
      i->first.shard);
    msg->map_epoch = get_parent()->get_epoch();
    msg->min_epoch = get_parent()->get_interval_start_epoch();
    msg->op = i->second;
    msg->op.from = get_parent()->whoami_shard();
    msg->op.tid = tid;
    if (op.trace) {
      // initialize a child span for this shard
      msg->trace.init("ec sub read", nullptr, &op.trace);
      msg->trace.keyval("shard", i->first.shard.id);
    }
    get_parent()->send_message_osd_cluster(
      i->first.osd, // peer
      msg, // message
      get_parent()->get_epoch()); // from epoch
  }
  dout(10) << __func__ << ": started " << op << dendl;
}

ECUtil::HashInfoRef ECBackend::get_hash_info(
  const hobject_t &hoid, bool checks, const map<string,bufferptr> *attrs)
{
  dout(10) << __func__ << ": Getting attr on " << hoid << dendl;
  ECUtil::HashInfoRef ref = unstable_hashinfo_registry.lookup(hoid);
  if (!ref) {
    dout(10) << __func__ << ": not in cache " << hoid << dendl;
    struct stat st;
    int r = store->stat(
      ch,
      ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      &st);
    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    // XXX: What does it mean if there is no object on disk?
    if (r >= 0) {
      dout(10) << __func__ << ": found on disk, size " << st.st_size << dendl;
      bufferlist bl;
      if (attrs) {
	map<string, bufferptr>::const_iterator k = attrs->find(ECUtil::get_hinfo_key());
	if (k == attrs->end()) {
	  dout(5) << __func__ << " " << hoid << " missing hinfo attr" << dendl;
	} else {
	  bl.push_back(k->second);
	}
      } else {
	r = store->getattr(
	  ch,
	  ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
	  ECUtil::get_hinfo_key(),
	  bl);
	if (r < 0) {
	  dout(5) << __func__ << ": getattr failed: " << cpp_strerror(r) << dendl;
	  bl.clear(); // just in case
	}
      }
      if (bl.length() > 0) {
	bufferlist::iterator bp = bl.begin();
	::decode(hinfo, bp);
	if (checks && hinfo.get_total_chunk_size() != (uint64_t)st.st_size) {
	  dout(0) << __func__ << ": Mismatch of total_chunk_size "
			       << hinfo.get_total_chunk_size() << dendl;
	  return ECUtil::HashInfoRef();
	}
      } else if (st.st_size > 0) { // If empty object and no hinfo, create it
	return ECUtil::HashInfoRef();
      }
    }
    ref = unstable_hashinfo_registry.lookup_or_create(hoid, hinfo);
  }
  return ref;
}

void ECBackend::start_rmw(Op *op, PGTransactionUPtr &&t)
{
  assert(op);

  op->plan = ECTransaction::get_write_plan(
    sinfo,
    std::move(t),
    [&](const hobject_t &i) {
      ECUtil::HashInfoRef ref = get_hash_info(i, false);
      if (!ref) {
	derr << __func__ << ": get_hash_info(" << i << ")"
	     << " returned a null pointer and there is no "
	     << " way to recover from such an error in this "
	     << " context" << dendl;
	ceph_abort();
      }
      return ref;
    },
    get_parent()->get_dpp());

  dout(10) << __func__ << ": " << *op << dendl;

  waiting_state.push_back(*op);
  check_ops();
}

bool ECBackend::try_state_to_reads()
{
  if (waiting_state.empty())
    return false;

  Op *op = &(waiting_state.front());
  if (op->requires_rmw() && pipeline_state.cache_invalid()) {
    assert(get_parent()->get_pool().allows_ecoverwrites());
    dout(20) << __func__ << ": blocking " << *op
	     << " because it requires an rmw and the cache is invalid "
	     << pipeline_state
	     << dendl;
    return false;
  }

  if (op->invalidates_cache()) {
    dout(20) << __func__ << ": invalidating cache after this op"
	     << dendl;
    pipeline_state.invalidate();
    op->using_cache = false;
  } else {
    op->using_cache = pipeline_state.caching_enabled();
  }

  waiting_state.pop_front();
  waiting_reads.push_back(*op);

  if (op->using_cache) {
    cache.open_write_pin(op->pin);

    extent_set empty;
    for (auto &&hpair: op->plan.will_write) {
      auto to_read_plan_iter = op->plan.to_read.find(hpair.first);
      const extent_set &to_read_plan =
	to_read_plan_iter == op->plan.to_read.end() ?
	empty :
	to_read_plan_iter->second;

      extent_set remote_read = cache.reserve_extents_for_rmw(
	hpair.first,
	op->pin,
	hpair.second,
	to_read_plan);

      extent_set pending_read = to_read_plan;
      pending_read.subtract(remote_read);

      if (!remote_read.empty()) {
	op->remote_read[hpair.first] = std::move(remote_read);
      }
      if (!pending_read.empty()) {
	op->pending_read[hpair.first] = std::move(pending_read);
      }
    }
  } else {
    op->remote_read = op->plan.to_read;
  }

  dout(10) << __func__ << ": " << *op << dendl;

  if (!op->remote_read.empty()) {
    assert(get_parent()->get_pool().allows_ecoverwrites());
    objects_read_async_no_cache(
      op->remote_read,
      [this, op](map<hobject_t,pair<int, extent_map> > &&results) {
	for (auto &&i: results) {
	  op->remote_read_result.emplace(i.first, i.second.second);
	}
	check_ops();
      });
  }

  return true;
}

bool ECBackend::try_reads_to_commit()
{
  if (waiting_reads.empty())
    return false;
  Op *op = &(waiting_reads.front());
  if (op->read_in_progress())
    return false;
  waiting_reads.pop_front();
  waiting_commit.push_back(*op);

  dout(10) << __func__ << ": starting commit on " << *op << dendl;
  dout(20) << __func__ << ": " << cache << dendl;

  get_parent()->apply_stats(
    op->hoid,
    op->delta_stats);

  if (op->using_cache) {
    for (auto &&hpair: op->pending_read) {
      op->remote_read_result[hpair.first].insert(
	cache.get_remaining_extents_for_rmw(
	  hpair.first,
	  op->pin,
	  hpair.second));
    }
    op->pending_read.clear();
  } else {
    assert(op->pending_read.empty());
  }

  map<shard_id_t, ObjectStore::Transaction> trans;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    trans[i->shard];
  }

  op->trace.event("start ec write");

  map<hobject_t,extent_map> written;
  if (op->plan.t) {
    ECTransaction::generate_transactions(
      op->plan,
      ec_impl,
      get_parent()->get_info().pgid.pgid,
      (get_osdmap()->require_osd_release < CEPH_RELEASE_KRAKEN),
      sinfo,
      op->remote_read_result,
      op->log_entries,
      &written,
      &trans,
      &(op->temp_added),
      &(op->temp_cleared),
      get_parent()->get_dpp());
  }

  dout(20) << __func__ << ": " << cache << dendl;
  dout(20) << __func__ << ": written: " << written << dendl;
  dout(20) << __func__ << ": op: " << *op << dendl;

  if (!get_parent()->get_pool().allows_ecoverwrites()) {
    for (auto &&i: op->log_entries) {
      if (i.requires_kraken()) {
	derr << __func__ << ": log entry " << i << " requires kraken"
	     << " but overwrites are not enabled!" << dendl;
	ceph_abort();
      }
    }
  }

  map<hobject_t,extent_set> written_set;
  for (auto &&i: written) {
    written_set[i.first] = i.second.get_interval_set();
  }
  dout(20) << __func__ << ": written_set: " << written_set << dendl;
  assert(written_set == op->plan.will_write);

  if (op->using_cache) {
    for (auto &&hpair: written) {
      dout(20) << __func__ << ": " << hpair << dendl;
      cache.present_rmw_update(hpair.first, op->pin, hpair.second);
    }
  }
  op->remote_read.clear();
  op->remote_read_result.clear();

  dout(10) << "onreadable_sync: " << op->on_local_applied_sync << dendl;
  ObjectStore::Transaction empty;
  bool should_write_local = false;
  ECSubWrite local_write_op;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    op->pending_apply.insert(*i);
    op->pending_commit.insert(*i);
    map<shard_id_t, ObjectStore::Transaction>::iterator iter =
      trans.find(i->shard);
    assert(iter != trans.end());
    bool should_send = get_parent()->should_send_op(*i, op->hoid);
    const pg_stat_t &stats =
      should_send ?
      get_info().stats :
      parent->get_shard_info().find(*i)->second.stats;

    ECSubWrite sop(
      get_parent()->whoami_shard(),
      op->tid,
      op->reqid,
      op->hoid,
      stats,
      should_send ? iter->second : empty,
      op->version,
      op->trim_to,
      op->roll_forward_to,
      op->log_entries,
      op->updated_hit_set_history,
      op->temp_added,
      op->temp_cleared,
      !should_send);

    ZTracer::Trace trace;
    if (op->trace) {
      // initialize a child span for this shard
      trace.init("ec sub write", nullptr, &op->trace);
      trace.keyval("shard", i->shard.id);
    }

    if (*i == get_parent()->whoami_shard()) {
      should_write_local = true;
      local_write_op.claim(sop);
    } else {
      MOSDECSubOpWrite *r = new MOSDECSubOpWrite(sop);
      r->pgid = spg_t(get_parent()->primary_spg_t().pgid, i->shard);
      r->map_epoch = get_parent()->get_epoch();
      r->min_epoch = get_parent()->get_interval_start_epoch();
      r->trace = trace;
      get_parent()->send_message_osd_cluster(
	i->osd, r, get_parent()->get_epoch());
    }
  }
  if (should_write_local) {
      handle_sub_write(
	get_parent()->whoami_shard(),
	op->client_op,
	local_write_op,
	op->trace,
	op->on_local_applied_sync);
      op->on_local_applied_sync = 0;
  }

  for (auto i = op->on_write.begin();
       i != op->on_write.end();
       op->on_write.erase(i++)) {
    (*i)();
  }

  return true;
}

bool ECBackend::try_finish_rmw()
{
  if (waiting_commit.empty())
    return false;
  Op *op = &(waiting_commit.front());
  if (op->write_in_progress())
    return false;
  waiting_commit.pop_front();

  dout(10) << __func__ << ": " << *op << dendl;
  dout(20) << __func__ << ": " << cache << dendl;

  if (op->roll_forward_to > completed_to)
    completed_to = op->roll_forward_to;
  if (op->version > committed_to)
    committed_to = op->version;

  if (get_osdmap()->require_osd_release >= CEPH_RELEASE_KRAKEN) {
    if (op->version > get_parent()->get_log().get_can_rollback_to() &&
	waiting_reads.empty() &&
	waiting_commit.empty()) {
      // submit a dummy transaction to kick the rollforward
      auto tid = get_parent()->get_tid();
      Op *nop = &(tid_to_op_map[tid]);
      nop->hoid = op->hoid;
      nop->trim_to = op->trim_to;
      nop->roll_forward_to = op->version;
      nop->tid = tid;
      nop->reqid = op->reqid;
      waiting_reads.push_back(*nop);
    }
  }

  if (op->using_cache) {
    cache.release_write_pin(op->pin);
  }
  tid_to_op_map.erase(op->tid);

  if (waiting_reads.empty() &&
      waiting_commit.empty()) {
    pipeline_state.clear();
    dout(20) << __func__ << ": clearing pipeline_state "
	     << pipeline_state
	     << dendl;
  }
  return true;
}

void ECBackend::check_ops()
{
  while (try_state_to_reads() ||
	 try_reads_to_commit() ||
	 try_finish_rmw());
}

int ECBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  uint32_t op_flags,
  bufferlist *bl)
{
  return -EOPNOTSUPP;
}

void ECBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
             pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete,
  bool fast_read)
{
  map<hobject_t,std::list<boost::tuple<uint64_t, uint64_t, uint32_t> > >
    reads;

  uint32_t flags = 0;
  extent_set es;
  for (list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
	 pair<bufferlist*, Context*> > >::const_iterator i =
	 to_read.begin();
       i != to_read.end();
       ++i) {
    pair<uint64_t, uint64_t> tmp =
      sinfo.offset_len_to_stripe_bounds(
	make_pair(i->first.get<0>(), i->first.get<1>()));

    extent_set esnew;
    esnew.insert(tmp.first, tmp.second);
    es.union_of(esnew);
    flags |= i->first.get<2>();
  }

  if (!es.empty()) {
    auto &offsets = reads[hoid];
    for (auto j = es.begin();
	 j != es.end();
	 ++j) {
      offsets.push_back(
	boost::make_tuple(
	  j.get_start(),
	  j.get_len(),
	  flags));
    }
  }

  struct cb {
    ECBackend *ec;
    hobject_t hoid;
    list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
	      pair<bufferlist*, Context*> > > to_read;
    unique_ptr<Context> on_complete;
    cb(const cb&) = delete;
    cb(cb &&) = default;
    cb(ECBackend *ec,
       const hobject_t &hoid,
       const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
                  pair<bufferlist*, Context*> > > &to_read,
       Context *on_complete)
      : ec(ec),
	hoid(hoid),
	to_read(to_read),
	on_complete(on_complete) {}
    void operator()(map<hobject_t,pair<int, extent_map> > &&results) {
      auto dpp = ec->get_parent()->get_dpp();
      ldpp_dout(dpp, 20) << "objects_read_async_cb: got: " << results
			 << dendl;
      ldpp_dout(dpp, 20) << "objects_read_async_cb: cache: " << ec->cache
			 << dendl;

      auto &got = results[hoid];

      int r = 0;
      for (auto &&read: to_read) {
	if (got.first < 0) {
	  if (read.second.second) {
	    read.second.second->complete(got.first);
	  }
	  if (r == 0)
	    r = got.first;
	} else {
	  assert(read.second.first);
	  uint64_t offset = read.first.get<0>();
	  uint64_t length = read.first.get<1>();
	  auto range = got.second.get_containing_range(offset, length);
	  assert(range.first != range.second);
	  assert(range.first.get_off() <= offset);
	  assert(
	    (offset + length) <=
	    (range.first.get_off() + range.first.get_len()));
	  read.second.first->substr_of(
	    range.first.get_val(),
	    offset - range.first.get_off(),
	    length);
	  if (read.second.second) {
	    read.second.second->complete(length);
	    read.second.second = nullptr;
	  }
	}
      }
      to_read.clear();
      if (on_complete) {
	on_complete.release()->complete(r);
      }
    }
    ~cb() {
      for (auto &&i: to_read) {
	delete i.second.second;
      }
      to_read.clear();
    }
  };
  objects_read_and_reconstruct(
    reads,
    fast_read,
    make_gen_lambda_context<
      map<hobject_t,pair<int, extent_map> > &&, cb>(
	cb(this,
	   hoid,
	   to_read,
	   on_complete)));
}

struct CallClientContexts :
  public GenContext<pair<RecoveryMessages*, ECBackend::read_result_t& > &> {
  hobject_t hoid;
  ECBackend *ec;
  ECBackend::ClientAsyncReadStatus *status;
  list<boost::tuple<uint64_t, uint64_t, uint32_t> > to_read;
  CallClientContexts(
    hobject_t hoid,
    ECBackend *ec,
    ECBackend::ClientAsyncReadStatus *status,
    const list<boost::tuple<uint64_t, uint64_t, uint32_t> > &to_read)
    : hoid(hoid), ec(ec), status(status), to_read(to_read) {}
  void finish(pair<RecoveryMessages *, ECBackend::read_result_t &> &in) override {
    // MatanLiramV1 assertion failed
    ECBackend::read_result_t &res = in.second;
    extent_map result;
    if (res.r != 0)
      goto out;
    // the 0 sized read of parity chunk returns a -EINVAL, we would just ignore
    // that for the system to work properly...
    // dout won't compile here so we move it to complete(arg) call
    assert(res.returned.size() == to_read.size());
    assert(res.r == 0);
    assert(res.errors.empty());
    for (auto &&read: to_read) {
      pair<uint64_t, uint64_t> adjusted =
	ec->sinfo.offset_len_to_stripe_bounds(
	  make_pair(read.get<0>(), read.get<1>()));
      assert(res.returned.front().get<0>() == adjusted.first &&
	     res.returned.front().get<1>() == adjusted.second);
      map<int, bufferlist> to_decode;
      bufferlist bl;
      for (map<pg_shard_t, bufferlist>::iterator j =
	     res.returned.front().get<2>().begin();
	   j != res.returned.front().get<2>().end();
	   ++j) {
	to_decode[j->first.shard].claim(j->second);
      }
      // Not for recovery, only returns bufferlist with data chunks
      int r = ECUtil::decode(
	ec->sinfo,
	ec->ec_impl,
	to_decode,
	&bl);
      if (r < 0) {
        res.r = r;
        goto out;
      }
      bufferlist trimmed;
      trimmed.substr_of(
	bl,
	read.get<0>() - adjusted.first,
	MIN(read.get<1>(),
	    bl.length() - (read.get<0>() - adjusted.first)));
      result.insert(
	read.get<0>(), trimmed.length(), std::move(trimmed));
      res.returned.pop_front();
    }
out:
    status->complete_object(hoid, res.r, std::move(result));
    ec->kick_reads();
  }
};

void ECBackend::objects_read_and_reconstruct(
  const map<hobject_t,
    std::list<boost::tuple<uint64_t, uint64_t, uint32_t> >
  > &reads,
  bool fast_read,
  GenContextURef<map<hobject_t,pair<int, extent_map> > &&> &&func)
{
  in_progress_client_reads.emplace_back(
    reads.size(), std::move(func));
  if (!reads.size()) {
    kick_reads();
    return;
  }

  set<int> want_to_read;
  get_want_to_read_shards(&want_to_read);

  map<hobject_t, read_request_t> for_read_op;
  for (auto &&to_read: reads) {
    set<pg_shard_t> shards;
    dout(1) << __func__ << "flow_y: initializing elements_map" << dendl;
    map<int, set<int> > elements_map = map<int, set<int> >();
    int r = get_min_avail_to_read_shards(
      to_read.first,
      want_to_read,
      false,
      fast_read,
      &shards,
      elements_map);
    assert(r == 0);
    // Possibly edit offsets here to remove redundant places.
    // Only matters if we measure degraded reads.

    CallClientContexts *c = new CallClientContexts(
      to_read.first,
      this,
      &(in_progress_client_reads.back()),
      to_read.second);
    // MatanLiramV5: send elements_map in read request
    for_read_op.insert(
      make_pair(
	to_read.first,
	read_request_t(
	  to_read.second,
	  shards,
	  false,
	  c,
          elements_map)));
  }

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    for_read_op,
    OpRequestRef(),
    fast_read, false);
  return;
}


int ECBackend::send_all_remaining_reads(
  const hobject_t &hoid,
  ReadOp &rop)
{
  set<int> already_read;
  const set<pg_shard_t>& ots = rop.obj_to_source[hoid];
  for (set<pg_shard_t>::iterator i = ots.begin(); i != ots.end(); ++i)
    already_read.insert(i->shard);
  dout(10) << __func__ << " have/error shards=" << already_read << dendl;
  set<pg_shard_t> shards;
  int r = get_remaining_shards(hoid, already_read, &shards);
  if (r)
    return r;
  if (shards.empty())
    return -EIO;

  dout(10) << __func__ << " Read remaining shards " << shards << dendl;

  // TODOSAM: this doesn't seem right
  list<boost::tuple<uint64_t, uint64_t, uint32_t> > offsets =
    rop.to_read.find(hoid)->second.to_read;
  GenContext<pair<RecoveryMessages *, read_result_t& > &> *c =
    rop.to_read.find(hoid)->second.cb;

  map<hobject_t, read_request_t> for_read_op;
  for_read_op.insert(
    make_pair(
      hoid,
      read_request_t(
	offsets,
	shards,
	false,
	c)));

  rop.to_read.swap(for_read_op);
  do_read_op(rop);
  return 0;
}

int ECBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist> *out)
{
  int r = store->getattrs(
    ch,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    *out);
  if (r < 0)
    return r;

  for (map<string, bufferlist>::iterator i = out->begin();
       i != out->end();
       ) {
    if (ECUtil::is_hinfo_key_string(i->first))
      out->erase(i++);
    else
      ++i;
  }
  return r;
}

void ECBackend::rollback_append(
  const hobject_t &hoid,
  uint64_t old_size,
  ObjectStore::Transaction *t)
{
  assert(old_size % sinfo.get_stripe_width() == 0);
  t->truncate(
    coll,
    ghobject_t(hoid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
    sinfo.aligned_logical_offset_to_chunk_offset(
      old_size));
}

void ECBackend::be_deep_scrub(
  const hobject_t &poid,
  uint32_t seed,
  ScrubMap::object &o,
  ThreadPool::TPHandle &handle) {
  bufferhash h(-1); // we always used -1
  int r;
  uint64_t stride = cct->_conf->osd_deep_scrub_stride;
  if (stride % sinfo.get_chunk_size())
    stride += sinfo.get_chunk_size() - (stride % sinfo.get_chunk_size());
  uint64_t pos = 0;

  uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

  while (true) {
    bufferlist bl;
    handle.reset_tp_timeout();
    r = store->read(
      ch,
      ghobject_t(
	poid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard),
      pos,
      stride, bl,
      fadvise_flags);
    if (r < 0)
      break;
    if (bl.length() % sinfo.get_chunk_size()) {
      r = -EIO;
      break;
    }
    pos += r;
    h << bl;
    if ((unsigned)r < stride)
      break;
  }

  if (r == -EIO) {
    dout(0) << "_scan_list  " << poid << " got "
	    << r << " on read, read_error" << dendl;
    o.read_error = true;
    return;
  }

  ECUtil::HashInfoRef hinfo = get_hash_info(poid, false, &o.attrs);
  if (!hinfo) {
    dout(0) << "_scan_list  " << poid << " could not retrieve hash info" << dendl;
    o.read_error = true;
    o.digest_present = false;
    return;
  } else {
    if (!get_parent()->get_pool().allows_ecoverwrites()) {
      assert(hinfo->has_chunk_hash());
      if (hinfo->get_total_chunk_size() != pos) {
	dout(0) << "_scan_list  " << poid << " got incorrect size on read" << dendl;
	o.ec_size_mismatch = true;
	return;
      }

      if (hinfo->get_chunk_hash(get_parent()->whoami_shard().shard) != h.digest()) {
	dout(0) << "_scan_list  " << poid << " got incorrect hash on read" << dendl;
	o.ec_hash_mismatch = true;
	return;
      }

      /* We checked above that we match our own stored hash.  We cannot
       * send a hash of the actual object, so instead we simply send
       * our locally stored hash of shard 0 on the assumption that if
       * we match our chunk hash and our recollection of the hash for
       * chunk 0 matches that of our peers, there is likely no corruption.
       */
      o.digest = hinfo->get_chunk_hash(0);
      o.digest_present = true;
    } else {
      /* Hack! We must be using partial overwrites, and partial overwrites
       * don't support deep-scrub yet
       */
      o.digest = 0;
      o.digest_present = true;
    }
  }

  o.omap_digest = seed;
  o.omap_digest_present = true;
}
