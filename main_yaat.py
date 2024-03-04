import dropbox

dbx = dropbox.Dropbox('sl.BwwHSuAQqUBMp2d32i5rmzrlDnKMNSVSSmsCHnWhMmavmXPd0AcQxfy42QIeyUDJWbwSRO3IqEzK_kFCV3UWcAMhPhTShwlQM6jZM-0KLQnUctpQ42pXYVRRjWmlOsXumOczW4sHZjP9')
print(dbx.users_get_current_account())
try: dbx.files_delete_v2('/cavs vs warriors')
except: pass
for entry in dbx.files_list_folder('').entries: print(entry.name, '\n')