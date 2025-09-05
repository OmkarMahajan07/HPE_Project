package auth

import "strings"

// UserPermissions defines what operations a user can perform
type UserPermissions struct {
	Read     bool
	Write    bool
	Delete   bool
	Create   bool
	List     bool
	Rename   bool
	Resume   bool
	Compress bool
	Parallel bool
}

// ParsePermissions parses a permission string (e.g., "RWDCL") into a UserPermissions struct
func ParsePermissions(permStr string) UserPermissions {
	perms := UserPermissions{}
	permStr = strings.ToUpper(permStr)

	perms.Read = strings.Contains(permStr, "R")
	perms.Write = strings.Contains(permStr, "W")
	perms.Delete = strings.Contains(permStr, "D")
	perms.Create = strings.Contains(permStr, "C")
	perms.List = strings.Contains(permStr, "L")
	perms.Rename = strings.Contains(permStr, "N")
	perms.Resume = strings.Contains(permStr, "S")
	perms.Compress = strings.Contains(permStr, "Z")
	perms.Parallel = strings.Contains(permStr, "P")

	return perms
}

// HasPermission checks if a user has a specific permission
func (p *UserPermissions) HasPermission(permission string) bool {
	switch strings.ToUpper(permission) {
	case "READ":
		return p.Read
	case "WRITE":
		return p.Write
	case "DELETE":
		return p.Delete
	case "CREATE":
		return p.Create
	case "LIST":
		return p.List
	case "RENAME":
		return p.Rename
	case "RESUME":
		return p.Resume
	case "COMPRESS":
		return p.Compress
	case "PARALLEL":
		return p.Parallel
	default:
		return false
	}
}
