#ifndef EDBA_DEFS_H
#define EDBA_DEFS_H

#if defined(WIN32) || defined(_WIN32) || defined(__WIN32) || defined(__CYGWIN__)
#  if defined(edba_EXPORTS)
#    ifdef EDBA_SOURCE
#      define EDBA_API __declspec(dllexport)
#    else
#      define EDBA_API __declspec(dllimport)
#    endif
#  endif
#  if defined(edba_DRIVER_EXPORTS)
#    ifdef EDBA_DRIVER_SOURCE
#      define EDBA_DRIVER_API __declspec(dllexport)
#    else
#      define EDBA_DRIVER_API __declspec(dllimport)
#    endif
#  endif
#  pragma warning(disable : 4275 4251)
#endif


#ifndef EDBA_API
#  define EDBA_API
#endif

#ifndef EDBA_DRIVER_API
#  define EDBA_DRIVER_API
#endif

#endif
