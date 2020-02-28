#include <sys/stat.h>
#include <ftw.h>
#include <unistd.h>

#include "broker/config.hh"

#ifdef BROKER_BSD
#include <sys/syslimits.h>
#endif

#include <cerrno>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

#include "broker/detail/die.hh"
#include "broker/detail/filesystem.hh"

namespace broker {
namespace detail {

bool exists(const path& p) {
  struct stat st;
  return ::lstat(p.c_str(), &st) == 0;
}

bool is_directory(const path& p) {
  struct stat sb;
  return stat(p.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode);
}

bool is_file(const path& p) {
  struct stat sb;
  return stat(p.c_str(), &sb) == 0 && S_ISREG(sb.st_mode);
}

namespace {

std::vector<std::string> tokenize(std::string input, const std::string delim) {
  std::vector<std::string> rval;
  size_t n;

  while ( (n = input.find(delim)) != std::string::npos ) {
    rval.push_back(input.substr(0, n));
    input.erase(0, n + 1);
  }

  rval.push_back(input);
  return rval;
}

}  // namespace <anonymous>

bool mkdirs(const path& p) {
  const mode_t perms = 0777;

  if ( p.empty() )
    return true;

  path dir_to_make = "";

  for ( auto& pc : tokenize(p, "/") ) {
    dir_to_make += pc;
    dir_to_make += "/";

    if ( ::mkdir(dir_to_make.c_str(), perms) < 0 ) {

      if ( errno == EISDIR )
          continue;

      if ( errno == EEXIST && is_directory(dir_to_make) )
        continue;

      return false;
    }
  }

  return true;
}

path dirname(const path& p) {
  auto last_slash = p.find_last_of('/');

  if ( last_slash == path::npos )
    return "";

  return p.substr(0, last_slash);
}

bool remove(const path& p) {
  return remove_all(p); // lazy way out
}

namespace {

int rm(const char* path, const struct stat*, int, FTW*) {
  return ::remove(path);
}

std::once_flag openmax_flag;

// Portable solution to retrieve the value of OPEN_MAX.
// Adapted from: http://stackoverflow.com/a/8225250
long open_max() {
#ifdef OPEN_MAX
  static long openmax = OPEN_MAX;
#else
  static long openmax = 0;
#endif
  std::call_once(openmax_flag, [&] {
    if (openmax == 0) {
      errno = 0;
      if ((openmax = sysconf(_SC_OPEN_MAX)) < 0) {
        if (errno == 0)
          openmax = 256; // Guess a value.
        else
          die("open_max: sysconf(_SC_OPEN_MAX)");
      }
    }
  });
  return openmax;
}

} // namespace <anonymous>

bool remove_all(const path& p) {
  struct stat st;
  if (::lstat(p.c_str(), &st) != 0)
    return false;
  if (S_ISDIR(st.st_mode))
    return ::nftw(p.c_str(), rm, open_max(), FTW_DEPTH | FTW_PHYS) == 0;
  else
    return ::remove(p.c_str()) == 0;
}

std::vector<std::string> readlines(const path& p, bool keep_empties) {
  std::vector<std::string> result;
  std::string line;
  std::ifstream f{p};
  while (std::getline(f, line))
    if (!line.empty() || keep_empties)
      result.emplace_back(line);
  return result;
}

std::string read(const path& p) {
  std::ifstream f{p};
  return std::string{std::istreambuf_iterator<char>(f),
                     std::istreambuf_iterator<char>()};
}

} // namespace detail
} // namespace broker
