/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */


/**
 * @file   ErasureCodePluginZigzag.cc
 *
 * @brief  Erasure Code Plug-in class wrapping the zigzag library
 *
 * The factory plug-in class allows to call individual encoding techniques.
 * The zigzag library provides various duplication, permutation, stripe size,
 * alignment and etc. configurations.
 */


#include "ceph_ver.h"
#include "common/debug.h"
#include "ErasureCodePluginZigzag.h"
#include "ErasureCodeZigzag.h"

// re-include our assert
#include "include/assert.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginZigzag: ";
}

int ErasureCodePluginZigzag::factory(const std::string &directory,
		      ErasureCodeProfile &profile,
		      ErasureCodeInterfaceRef *erasure_code,
		      std::ostream *ss)
{
    ErasureCodeZigzag *interface;
    interface = new ErasureCodeZigzag();
    int r = interface->init(profile, ss);
    if (r) {
      delete interface;
      return r;
    }
    *erasure_code = ErasureCodeInterfaceRef(interface);
    return 0;
};

#ifndef BUILDING_FOR_EMBEDDED

const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

int __erasure_code_init(char *plugin_name, char *directory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginZigzag());
}

#endif

