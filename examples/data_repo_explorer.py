import getpass

import qsig

# Basic example of using the DataRepo to example libraries and items

repo = qsig.DataRepo(f"/home/{getpass.getuser()}/DATAREPO")

libs = repo.list_libraries()
print(f"REPO: {repo}")
print("LIBS:")
for lib in libs:
    print(f"* {lib}")

print("ITEMS:")
for lib_name in libs:
    lib = repo.get_library(lib_name)
    keys = lib.list_keys()
    print(f"* {lib_name} (key-count: {len(keys)})")
    for key in keys:
        print(f"  - {key}")
