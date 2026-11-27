#!/usr/bin/env bash
# Builds a per-platform JVM CLI package: the portable wvlet-cli-<version>.tar.gz plus a Java
# runtime made by jlink in runtime/, which the wvlet and wv launchers prefer over JAVA_HOME and
# PATH. Run it on the target platform: the runtime is linked from the JDK that runs this script.
# See design/releasing.md.
#
# Usage: add-jlink-runtime.sh <wvlet-cli-<version>.tar.gz> <output.tar.gz>
# The JDK is taken from JAVA_HOME, or from the java on PATH.
set -euo pipefail

# JDK modules of the lite CLI. java.desktop is required only by the PostgreSQL driver
# (java.beans), and jdk.jfr by Netty's JFR events; jdk.charsets and jdk.zipfs are loaded as
# services, which jdeps does not report.
MODULES="java.base,java.desktop,java.logging,java.management,java.naming,java.net.http"
MODULES="${MODULES},java.security.jgss,java.sql,java.transaction.xa,java.xml,jdk.jfr"
MODULES="${MODULES},jdk.unsupported,jdk.charsets,jdk.zipfs"

if [ $# -ne 2 ]; then
  echo "usage: $0 <wvlet-cli-<version>.tar.gz> <output.tar.gz>" >&2
  exit 2
fi
input=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
output_dir=$(mkdir -p "$(dirname "$2")" && cd "$(dirname "$2")" && pwd)
output="${output_dir}/$(basename "$2")"

if [ -z "${JAVA_HOME:-}" ]; then
  # Ask the JVM: on macOS, /usr/bin/java is a stub outside any JDK
  JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 | awk -F' = ' '/^ *java.home = / { print $2 }')
fi
for tool in java jdeps jlink; do
  if [ ! -x "${JAVA_HOME}/bin/${tool}" ]; then
    echo "error: ${JAVA_HOME}/bin/${tool} not found; set JAVA_HOME to a JDK" >&2
    exit 1
  fi
done
java_feature=$("${JAVA_HOME}/bin/java" -XshowSettings:properties -version 2>&1 |
  awk -F' = ' '/java.specification.version/ { print $2 }')
echo "Linking the runtime from ${JAVA_HOME} (Java ${java_feature})"

# Resolve symlinks (macOS /var -> /private/var) so that the java.home check below matches
work=$(cd "$(mktemp -d)" && pwd -P)
trap 'rm -rf "${work}"' EXIT
tar -xzf "${input}" -C "${work}"
pack_name=$(ls "${work}")
pack="${work}/${pack_name}"
if [ ! -d "${pack}/lib" ] || [ -e "${pack}/runtime" ]; then
  echo "error: ${input} is not a portable wvlet-cli package" >&2
  exit 1
fi

# Fail when a jar needs a JDK module that the runtime leaves out, so that a new dependency breaks
# this build instead of failing with NoClassDefFoundError on a user's machine
# jdeps prints "<jar or module> -> <module>" per dependency; a jdeps error fails this assignment
# (set -e). Modular jars (Arrow) require other jars by module name, so pass them on a module path.
# Scala jars (*_3-*.jar) stay off it: "_3" makes an invalid automatic module name, and no module
# requires them.
modules_dir="${work}/module-path"
mkdir "${modules_dir}"
for jar in "${pack}"/lib/*.jar; do
  case "$(basename "${jar}")" in
    *_3-*) ;;
    *) ln -s "${jar}" "${modules_dir}/" ;;
  esac
done
deps=$("${JAVA_HOME}/bin/jdeps" --ignore-missing-deps --multi-release "${java_feature}" \
  --module-path "${modules_dir}" -summary -q "${pack}"/lib/*.jar)
missing=""
while read -r source _ module; do
  case "${module}" in java.* | jdk.*) ;; *) continue ;; esac
  case ",${MODULES}," in
    *",${module},"*) ;;
    *) missing="${missing}  ${source} -> ${module}"$'\n' ;;
  esac
done <<< "${deps}"
if [ -n "${missing}" ]; then
  echo "error: JDK modules missing from MODULES in $0:" >&2
  printf '%s' "${missing}" >&2
  exit 1
fi

"${JAVA_HOME}/bin/jlink" --add-modules "${MODULES}" --strip-debug --no-man-pages \
  --no-header-files --output "${pack}/runtime"

# Smoke test: the launcher must run the bundled runtime even when JAVA_HOME and PATH name
# another JDK. -XshowSettings:properties prints java.home before the query runs.
result=$(cd "${work}" && JAVA_HOME=/nonexistent JAVA_OPTS=-XshowSettings:properties \
  "${pack}/bin/wvlet" run -c 'from [[1],[2]] as t(x) select x * 2 as y' 2>&1) || {
  echo "${result}" >&2
  echo "error: the smoke query failed on the bundled runtime" >&2
  exit 1
}
if ! echo "${result}" | grep -q "java.home = ${pack}/runtime\$"; then
  echo "${result}" | grep 'java.home' >&2 || true
  echo "error: the launcher did not run the bundled runtime" >&2
  exit 1
fi
if ! echo "${result}" | grep -q '│ *4 │'; then
  echo "${result}" >&2
  echo "error: unexpected smoke query result" >&2
  exit 1
fi

# COPYFILE_DISABLE keeps macOS tar from adding ._* AppleDouble entries
COPYFILE_DISABLE=1 tar -czf "${output}" -C "${work}" "${pack_name}"
echo "Wrote ${output} ($(du -h "${output}" | cut -f1); runtime $(du -sh "${pack}/runtime" | cut -f1))"
