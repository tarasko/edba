#ifndef EDBA_DETAIL_EXPORTS_HPP
#define EDBA_DETAIL_EXPORTS_HPP

#if defined(_WIN32) || defined(__CYGWIN__)
#   define EDBA_HELPER_EXPORT __declspec(dllexport)
#   define EDBA_HELPER_IMPORT __declspec(dllimport)
#   define EDBA_HELPER_LOCAL
#else
#   if __GNUC__ >= 4
#       define EDBA_HELPER_EXPORT __attribute__ ((visibility ("default")))
#       define EDBA_HELPER_IMPORT __attribute__ ((visibility ("default")))
#       define EDBA_HELPER_LOCAL __attribute__ ((visibility ("hidden")))
#   else
#       define EDBA_HELPER_EXPORT
#       define EDBA_HELPER_IMPORT
#       define EDBA_HELPER_LOCAL
#   endif
#endif

#if defined(edba_EXPORTS)
#   define EDBA_API EDBA_HELPER_EXPORT
#elif !defined(edba_STATIC)
#   define EDBA_API EDBA_HELPER_IMPORT
#else
#   define EDBA_API
#endif

#if defined(edba_DRIVER_EXPORTS)
#   define EDBA_DRIVER_API EDBA_HELPER_EXPORT
#elif !defined(edba_DRIVER_STATIC)
#   define EDBA_DRIVER_API EDBA_HELPER_IMPORT
#else
#   define EDBA_DRIVER_API
#endif

#pragma warning(disable : 4275 4251)

#endif
