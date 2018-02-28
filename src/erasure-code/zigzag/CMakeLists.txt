# zigzag plugin

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
