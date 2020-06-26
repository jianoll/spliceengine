# docker_run.sh
#----------------------
# helper script to build/run spliceengine standalone

# examples:
#  bash docker_run.sh --- mvn install clean -Pcdh6.3.0,core -DskipTests
#  bash docker_run.sh -p 2527:1527 --- /bin/bash
# then, inside the container:
#  ./start-splice-cluster -pcdh6.3.0 -b

# note you can also do 
# bash docker_run.sh -p 2527:1527 --- ./start-splice-cluster
# however this will exit after the cluster is started, so no cluster anymore :-/

# to prevent this, start a cluster and attach a sqlshell to it, you need this helper script:
# bash docker_run.sh --- /bin/bash docker/start.sh <additional ./start-splice-cluster parameters>

# port forwarding:
# add port forwarding and other docker options before the ---
# bash docker_run.sh -p 1527:1527 -p 4000:4000 -p 4020:4020 -p 4025:4025 --- /bin/bash docker/start.sh -l

# shortcut for these mapping is --stdPortMap
# bash docker_run.sh --stdPortMap --- /bin/bash docker/start.sh -l

 # if you need mapping to different port, but all shifted, you canuse --portMap 1000, which will give you
 #  -p 2527:1527 -p 5000:4000 -p 5020:4020 -p 5025:4025


# options:
#  bash docker_run.sh --- mvn install clean -Pcdh6.3.0,core -DskipTests

# build only, use other maven root, copy source to temporary directory (clean dir build)
# bash docker_run.sh --port1527mapping off --setMavenHome /tmp/maven_home --setSource ${MY_TMP_SOURCE}--- mvn install clean -Pcdh6.3.0,core -DskipTests

# creating a full link to MY_TMP_SOURCE:
# MY_TMP_SOURCE=/tmp/my_source
#  mkdir ${MY_TMP_SOURCE}
#  cd $HOME/.m2
#  find . -type d -exec mkdir -p ${MY_TMP_SOURCE}/{} ';'
#  find . -type f -exec ln {} ${MY_TMP_SOURCE}/{} ';'
#  cd -


# docker_run.sh ./sqlshell.sh
# docker_start-splice-cluster.sh



docker build --tag se-spliceengine-build docker

# note that this will map $HOME/.m2 and $(pwd)/.. (=gitroot) into the container, and the container
# will also write to this directories.

# change with using --setMavenHome and --setSource

# default settings
OUR_PORT_MAP="OFF"
OUR_MAVEN_HOME="$HOME/.m2"
OUR_SOURCE=$(pwd)
OTHER_DOCKER_OPTIONS=""

while [ true ]
do
	if [ $1 == "--stdPortMap" ]; then
		OUR_PORT_MAP=0
		shift 1
	elif [ $1 == "--portMap" ]; then
		OUR_PORT_MAP=$2
		shift 2
	elif [ $1 == "--setMavenHome" ]; then
		OUR_MAVEN_HOME=$2
		shift 2
	elif [ $1 == "--setSource" ]; then
		OUR_SOURCE=$2
		shift 2
	elif [ $1 == "---" ]; then
		shift
		break
	else
		OTHER_DOCKER_OPTIONS="${OTHER_DOCKER_OPTIONS} $1"
		shift 1
	fi
done

# 1527 inside docker -> ${PORT_1527_MAPPING}
if [[ ${OUR_PORT_MAP} == "OFF" ]]; then
	CONFIG_PORT_MAPPING=""
else
	CONFIG_PORT_MAPPING="-p $((1527+OUR_PORT_MAP)):1527 -p $((4000+OUR_PORT_MAP)):4000 -p $((4020+OUR_PORT_MAP)):4020 -p $((4025+OUR_PORT_MAP)):4025"
fi

# reusing maven root from the host (avoid re-download)
CONFIG_MAVEN_VOLUME="-v ${OUR_MAVEN_HOME}:/root/.m2"

# map the gitroot of spliceengine to /usr/src
CONFIG_SOURCE_MAPPING="-v ${OUR_SOURCE}:/usr/src/"

# on linux: use current user
# otherwise if the container writes to mounted volumes they will have owner root
if [[ "$OSTYPE" == "darwin"* ]]; then
	CONFIG_USER_INFO="" # docker on macos writes per default as current user
else
	CONFIG_USER_INFO="--user $(id -u):$(id -g)"
fi

FULL_DOCKER_OPTIONS="${OTHER_DOCKER_OPTIONS} ${CONFIG_PORT_MAPPING} ${CONFIG_MAVEN_VOLUME} ${CONFIG_SOURCE_MAPPING} ${CONFIG_USER_INFO} \
-w /usr/src/ se-spliceengine-build $*"

echo
echo "--- STARTING DOCKER WITH FOLLOWING OPTIONS ---"
echo
echo "docker container run -it $FULL_DOCKER_OPTIONS"
echo
docker container run -it $(echo ${FULL_DOCKER_OPTIONS})

