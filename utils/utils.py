import os
import pathlib

def file_exists_and_has_data(file_path: pathlib):

    '''
    Given a file path, checks if the file exits and contains data
    '''

    file_exists = os.path.exists(file_path)

    if file_exists:
        if os.path.getsize(file_path) > 0:
            return True
        else:
            return False

    return False