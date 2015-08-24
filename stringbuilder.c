/**
 * Stringbuilder - a library for working with C strings that can grow dynamically as they are appended
 *
 */

#include "stringbuilder.h"

#include <string.h>
#include <stdio.h>

 /**
  * Formats the given format string and arguments and allocates a new string (ret)
  *
  * NOTE: I (Gabe Rudy) modified this heavily to use va_copy and work on LLVM
  */
int xp_asprintf(char** ret, const char* format, ...)   {
  char* str;
  int length;
  va_list ar1, ar2;

  // First determine the length by doing a "fake printf"
  va_start(ar1, format);
  va_copy(ar2, ar1);
  length = vsnprintf(0, 0, format, ar1);

  if (length <= 0)    {
    // Could not determine the space needed for the string
    va_end(ar2);
    va_end(ar1);
    return -1;
  }

  str = (char*)malloc(length + 1);
  if (!str)   {
    // Could not allocate enough memory for the string
    va_end(ar2);
    va_end(ar1);
    return -1;
  }

  length = vsnprintf(str, length + 1, format, ar2);
  va_end(ar2);
  va_end(ar1);

  *ret = str;

  return length;
}

/**
 * Creates a new stringbuilder with the default chunk size
 * 
 */
stringbuilder* sb_new() {
    return sb_new_with_size(1024);      // TODO: Is there a heurisitic for this?
}

/**
 * Creates a new stringbuilder with initial size at least the given size
 */
stringbuilder* sb_new_with_size(int size)   {
    stringbuilder* sb;
    
    sb = (stringbuilder*)malloc(sizeof(stringbuilder));
    sb->size = size;
    sb->cstr = (char*)malloc(size);
    sb->pos = 0;
    sb->reallocs = 0;
    
    return sb;
}

/**
 * Destroys the given stringbuilder.  Pass 1 to free_string if the underlying c string should also be freed
 */
void sb_destroy(stringbuilder* sb, int free_string) {
    if (free_string)    {
        free(sb->cstr);
    }
    
    free(sb);
}

/**
 * Appends at most length of the given src string to the string buffer
 */
void sb_append_strn(stringbuilder* sb, const char* src, int length) {
    int sdiff;
    
    sdiff = (length+1) - (sb->size - sb->pos);
    if (sdiff > 0)  {
        sb->size = sb->size + sdiff + (sdiff >> 2) + 1;
        sb->cstr = (char*)realloc(sb->cstr, sb->size);
        sb->reallocs++;
    }
    
    memcpy(sb->cstr + sb->pos, src, length);
    sb->pos += length;
    sb->cstr[sb->pos] = '\0'; //Keep cstring NULL terminated
}

/**
 * Appends the given src string to the string builder
 */
void sb_append_str(stringbuilder* sb, const char* src)  {
    sb_append_strn(sb, src, strlen(src));
}

/**
 * Allocates and copies a new cstring based on the current stringbuilder contents 
 */
char* sb_make_cstring(stringbuilder* sb)    {
    char* out;
    
    if (!sb->pos)   {
        return 0;
    }
    
    out = (char*)malloc(sb->pos + 1);
    strcpy(out, sb_cstring(sb));
    
    return out;
}

