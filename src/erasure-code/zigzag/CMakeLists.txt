# zigzag plugin


# Set the CFLAGS correctly for gf-complete based on SIMD compiler support
set(GF_COMPILE_FLAGS)
if(HAVE_ARMV8_SIMD)
  list(APPEND GF_COMPILE_FLAGS ARM_NEON ARCH_AARCH64)
endif()
if(HAVE_ARM_NEON)
  list(APPEND GF_COMPILE_FLAGS ARM_NEON)
endif()
if(HAVE_INTEL_SSE)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE)
endif()
if(HAVE_INTEL_SSE2)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE2)
endif()
if(HAVE_INTEL_SSE3)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE3)
endif()
if(HAVE_INTEL_SSSE3)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSSE3)
endif()
if(HAVE_INTEL_PCLMUL)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4_PCLMUL)
endif()
if(HAVE_INTEL_SSE4_1)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4)
endif()
if(HAVE_INTEL_SSE4_2)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4)
endif()

# set this to TRUE to enable debugging of SIMD detection
# inside gf-complete. gf-complete will printf the SIMD
# instructions detected to stdout.
if (FALSE)
  list(APPEND GF_COMPILE_FLAGS DEBUG_CPU_DETECTION)
endif()

set(gf-complete_srcs
  ../jerasure/gf-complete/src/gf_cpu.c
  ../jerasure/gf-complete/src/gf_wgen.c
  ../jerasure/gf-complete/src/gf_w16.c
  ../jerasure/gf-complete/src/gf.c
  ../jerasure/gf-complete/src/gf_w32.c
  ../jerasure/gf-complete/src/gf_w64.c
  ../jerasure/gf-complete/src/gf_w128.c
  ../jerasure/gf-complete/src/gf_general.c
  ../jerasure/gf-complete/src/gf_w4.c
  ../jerasure/gf-complete/src/gf_rand.c
  ../jerasure/gf-complete/src/gf_w8.c)


if(HAVE_ARM_NEON OR HAVE_ARMV8_SIMD)
  list(APPEND gf-complete_srcs
    ../jerasure/gf-complete/src/neon/gf_w4_neon.c
    ../jerasure/gf-complete/src/neon/gf_w8_neon.c
    ../jerasure/gf-complete/src/neon/gf_w16_neon.c
    ../jerasure/gf-complete/src/neon/gf_w32_neon.c
    ../jerasure/gf-complete/src/neon/gf_w64_neon.c)
endif()

add_library(gf-complete_objs OBJECT ${gf-complete_srcs})
set_target_properties(gf-complete_objs PROPERTIES
  COMPILE_FLAGS "${SIMD_COMPILE_FLAGS}")
set_target_properties(gf-complete_objs PROPERTIES
  COMPILE_DEFINITIONS "${GF_COMPILE_FLAGS}")

set(jerasure_srcs
  ../jerasure/jerasure/src/galois.c
  ../jerasure/jerasure/src/jerasure.c
  ../jerasure/jerasure/src/reed_sol.c
  ../jerasure/jerasure_init.cc)
add_library(jerasure_objs OBJECT ${jerasure_srcs})


set(zigzag_srcs
  ErasureCodePluginZigzag.cc
  ErasureCodeConfigurationsZigzag.cc
  ErasureCodeZigzag.cc
  $<TARGET_OBJECTS:crush_objs>
  $<TARGET_OBJECTS:gf-complete_objs>
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:erasure_code_objs>
)

add_library(ec_zigzag SHARED ${zigzag_srcs})
add_dependencies(ec_zigzag ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ec_zigzag PROPERTIES
  INSTALL_RPATH "")
target_link_libraries(ec_zigzag json_spirit)
install(TARGETS ec_zigzag DESTINATION ${erasure_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_ec_zigzag STATIC ${zigzag_srcs})
  set_target_properties(cephd_ec_zigzag PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
