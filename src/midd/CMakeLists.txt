# 查找当前目录下的所有源文件
# 并将名称保存到 DHMP_LIB_SRCS 变量
project(MIDD)
aux_source_directory(. MIDD_MICA_LIB_SRCS)
SET(LIBRARY_OUTPUT_PATH ${MIDD_MICA_SOURCE_DIR}/lib)
add_library (middlib ${MIDD_MICA_LIB_SRCS})

