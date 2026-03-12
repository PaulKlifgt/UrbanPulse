import ast
import json
import pprint
import sys

def patch_config(config_path, updates_path):
    with open(config_path, "r", encoding="utf-8") as f:
        source = f.read()

    with open(updates_path, "r", encoding="utf-8") as f:
        updates = json.load(f)

    # Convert true/false/null from json back to True/False/None via eval if needed
    # (though json.load already produces Python dicts)
    
    # We will format this dictionary to string
    # A simple custom formatter to look like original
    def format_dict(d, indent=2):
        ind = "    " * indent
        lines = ["{"]
        for k, v in d.items():
            if k == "preset_zones" and isinstance(v, list):
                lines.append(f'{ind}    "{k}": [')
                for item in v:
                    lines.append(f'{ind}        {repr(item)},')
                lines.append(f'{ind}    ],')
            elif isinstance(v, dict):
                lines.append(f'{ind}    "{k}": {format_dict(v, indent+1)},')
            else:
                lines.append(f'{ind}    "{k}": {repr(v)},')
        lines.append(f"{ind}}")
        return "\n".join(lines)
        
    cities_str = format_dict(updates, indent=1)

    # Use AST to locate the 'cities' key in CONFIG
    tree = ast.parse(source)
    config_dict = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and getattr(node.targets[0], "id", "") == "CONFIG":
            config_dict = node.value
            break

    if not config_dict:
        print("Could not find CONFIG dictionary")
        sys.exit(1)

    start_byte = -1
    end_byte = -1

    for k, v in zip(config_dict.keys, config_dict.values):
        if isinstance(k, ast.Constant) and k.value == "cities":
            # Extract line range of the dictionary
            # But the value covers the dict, wait - we need exact byte offsets if we want to replace exactly
            # Let's replace line-based
            start_line = v.lineno - 1
            end_line = v.end_lineno
            
            lines = source.split("\n")
            
            # The value could be on the same line as the key, e.g., "cities": {
            # So start_line is the line where { is.
            prefix = lines[start_line][:v.col_offset]
            suffix = lines[end_line-1][v.end_col_offset:]
            
            # Replace lines
            new_lines = lines[:start_line] + [prefix + cities_str + suffix] + lines[end_line:]
            
            with open(config_path, "w", encoding="utf-8") as fw:
                fw.write("\n".join(new_lines))
            print("Successfully patched config.py")
            return

    print("Could not find 'cities' key in CONFIG")

if __name__ == "__main__":
    patch_config("../config.py", "../config_updates.json")
