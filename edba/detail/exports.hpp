#ifndef EDBA_DEFS_H
#define EDBA_DEFS_H

#if defined(WIN32) || defined(_WIN32) || defined(__WIN32) || defined(__CYGWIN__)

#  if defined(edba_EXPORTS)
#    define EDBA_API __declspec(dllexport)
#  elif !defined(edba_STATIC)
#    define EDBA_API __declspec(dllimport)
#  else
#    define EDBA_API
#  endif

#  if defined(edba_DRIVER_EXPORTS)
#    define EDBA_DRIVER_API __declspec(dllexport)
#  elif !defined(edba_DRIVER_STATIC)
#    define EDBA_DRIVER_API __declspec(dllimport)
#  else
#    define EDBA_DRIVER_API
#  endif

#  pragma warning(disable : 4275 4251)

#else
#  define EDBA_API
#  define EDBA_DRIVER_API
#endif

#endif
