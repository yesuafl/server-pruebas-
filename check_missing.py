import re

with open(r"C:\Users\JD\AppData\Roaming\Code\User\History\-19c2baee\TM0m.html", "r", encoding="utf-8") as f:
    old = f.read()
with open(r"config_dashboard.html", "r", encoding="utf-8") as f:
    new = f.read()

old_ids = re.findall(r'id=\"([^\"]+)\"', old)
new_ids = re.findall(r'id=\"([^\"]+)\"', new)

missing = set(old_ids) - set(new_ids)
print("Missing IDs:", missing)

old_funcs = re.findall(r'function\s+([A-Za-z0-9_]+)\s*\(', old)
new_funcs = re.findall(r'function\s+([A-Za-z0-9_]+)\s*\(', new)

missing_funcs = set(old_funcs) - set(new_funcs)
print("Missing Functions:", missing_funcs)
