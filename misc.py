import os

def find_subfolders_with_prefix(parent_folder, prefix):
    """
    Find all subfolders of parent_folder that start with the given prefix.
    """
    matching_subfolders = []
    for root, dirs, files in os.walk(parent_folder):
        for folder_name in dirs:
            if folder_name.startswith(prefix):
                matching_subfolders.append(os.path.join(root, folder_name))
    if len(matching_subfolders) == 0:
        folder_name = os.path.basename(os.path.normpath(parent_folder))
        if folder_name.startswith(prefix):
            matching_subfolders.append(parent_folder)

    return matching_subfolders