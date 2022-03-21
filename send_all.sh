#!/bin/bash
###
 # @Author: your name
 # @Date: 2022-02-22 22:09:51
 # @LastEditTime: 2022-02-22 23:09:09
 # @LastEditors: Please set LastEditors
 # @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 # @FilePath: /star_midd/launch.sh
### 
scp -r ../LineKV  gtwang@node20:/home/gtwang/
scp -r ../LineKV  gtwang@node22:/home/gtwang/

scp  ./config.xml gtwang@node24:/home/gtwang/LineKV/config.xml
scp  ./bin/mica  gtwang@node24:/home/gtwang/LineKV
