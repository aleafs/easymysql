# !/bin/bash

export LANG=en_US.UTF-8

usage() {
    echo "`basename ${0}` [pid]"
}

if [ ${#} -lt 1 ] ; then
    usage
    exit 1
fi

declare -r pid="${1}"

check() {
    local now=`date +"%Y-%m-%d %H:%M:%S"`
    if [ "X" = "X${pid}" ] ; then
        return
    fi

    lsof -p "${pid}" | grep -w "ESTABLISHED" | grep "mysql" | awk '{print $(NF - 1)}' | \
        awk -F"[:>]" '{print $3}' | sort | uniq -c | \
        awk -v p="${pid}" -v t="${now}" '{printf "%s\t%d\t%s:%d\n", t, p, $2, $1}' 
}

check

