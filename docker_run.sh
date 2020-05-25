docker build --tag se-spliceengine-build docker

# note that this will map $HOME/.m2 and $(pwd)/.. (=gitroot) into the container, and the container
# will also write to this directories.
#
# TODO: when running on linux, the container will create files with user "root"
# this can be prevented with e.g. https://vsupalov.com/docker-shared-permissions/

HOST_PORT=2527

echo "starting $*"

docker run -it \
  -p ${HOST_PORT}:1527         `# 1527 inside docker -> ${HOST_PORT}`                   \
  -v "$HOME/.m2":/root/.m2     `# reusing maven root from the host (avoid re-download)` \
  -v "$(pwd)":/usr/src/        `# map the gitroot of spliceengine to /usr/src`          \
  -w /usr/src/                 `# set /usr/src as current working directoy`             \
  se-spliceengine-build \
  $*


# examples:
#  bash docker_run.sh mvn install clean -Pcdh6.3.0,core -DskipTests

#  bash docker_run.sh /bin/bash
# then, inside the container:
#  ./start-splice-cluster -pcdh6.3.0 -b

# bash docker_run.sh ./sqlshell.sh


# docker_run.sh mvn build
# docker_run.sh ./sqlshell.sh
# docker start-splice-cluster