# 查找当前目录下的所有源文件
# 并将名称保存到 DHMP_LIB_SRCS 变量
project(MIDD)
aux_source_directory(. MIDD_MICA_LIB_SRCS)
SET(LIBRARY_OUTPUT_PATH ${MIDD_MICA_SOURCE_DIR}/lib)
add_library (middlib ${MIDD_MICA_LIB_SRCS})

# list(REMOVE_ITEM MIDD_MICA_LIB_SRCS "./nic.c")
# add_library (middlib_witout_nic ${MIDD_MICA_LIB_SRCS})

# MESSAGE("MIDD_MICA_LIB_SRCS: ${MIDD_MICA_LIB_SRCS}")
