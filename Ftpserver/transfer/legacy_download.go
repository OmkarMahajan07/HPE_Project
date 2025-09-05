package transfer

// ==================== LEGACY DOWNLOAD FUNCTIONS ====================
// 
// NOTE: All download functions are currently being used by the main server:
//
// ✅ OpenMmapFile() - Used in ftp_server1.go line 2015
// ✅ tryWindowsMmap() - Used internally by OpenMmapFile()
// ✅ actualWindowsMmap() - Used internally by tryWindowsMmap()
//
// Unlike upload functions, the download functions form a complete call chain
// where each function depends on the others:
//
// OpenMmapFile() → tryWindowsMmap() → actualWindowsMmap()
//
// Therefore, no download functions have been moved to this legacy file.
// All download functions remain in download.go and are actively used.
//
// If future refactoring identifies unused download functions, they can be
// moved here for archival purposes.

// This file exists for consistency with legacy_upload.go and for potential
// future use if download functions become obsolete.
