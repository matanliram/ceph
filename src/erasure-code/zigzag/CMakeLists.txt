# zigzag plugin

include_directories(.)

set(zigzag_utils_src
  ErasureCodePluginZigzag.cc
  ErasureCodeZigzag.cc
  ErasureCodeConfigurationsZigzag.cc)

add_library(zigzag_utils OBJECT ${zigzag_utils_src})
add_dependencies(zigzag_utils ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

set(ec_zigzag_objs
  $<TARGET_OBJECTS:gf-complete_objs>
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:zigzag_utils>
  $<TARGET_OBJECTS:erasure_code_objs>)

add_library(ec_zigzag SHARED ${ec_zigzag_objs})
add_dependencies(ec_zigzag ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ec_zigzag PROPERTIES
  INSTALL_RPATH "")
target_link_libraries(ec_zigzag ${EXTRALIBS})
install(TARGETS ec_zigzag DESTINATION ${erasure_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_ec_zigzag STATIC
      $<TARGET_OBJECTS:gf-complete_objs>
      $<TARGET_OBJECTS:jerasure_objs>
      ${zigzag_utils_src})
  set_target_properties(cephd_ec_zigzag PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
