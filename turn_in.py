import argparse
import json
import os
import re
import shutil
import tempfile
import subprocess

PROJ_LIST = ['proj0', 'proj1', 'proj2', 'proj3_part1', 'proj3_part2', 'proj4_part1', 'proj4_part2', 'proj4_part3', 'proj5']

def check_student_id(student_id):
    m = re.match(r'[0-9]{8,10}', student_id)
    if not m or len(student_id) not in (8, 10):
        print('Error: Please double check that your student id is entered correctly. It should only include digits 0-9 and be of length 8 or 10.')
        exit()
    return student_id

def test_category(assignment):
    if assignment == 'proj3_part2':
            return '3'
    if assignment == 'proj4_part3':
            return '4'
    return assignment[4:].replace('_p', 'P')

def files_to_copy(assignment):
    files = {
        'proj0': ['src/main/java/edu/berkeley/cs186/database/databox/StringDataBox.java'],
        'proj1': ['proj1.sql'],
        'proj2': [
            'src/main/java/edu/berkeley/cs186/database/index/BPlusTree.java',
            'src/main/java/edu/berkeley/cs186/database/index/BPlusNode.java',
            'src/main/java/edu/berkeley/cs186/database/index/InnerNode.java',
            'src/main/java/edu/berkeley/cs186/database/index/LeafNode.java',
        ],
        'proj3_part1': [
            'src/main/java/edu/berkeley/cs186/database/query/BNLJOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/SortOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/SortMergeOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/GraceHashJoin.java',
        ],
        'proj3_part2': [
            'src/main/java/edu/berkeley/cs186/database/query/BNLJOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/SortOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/SortMergeOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/QueryPlan.java',
            'src/main/java/edu/berkeley/cs186/database/query/GraceHashJoin.java',
        ],
        'proj4_part1': [
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockType.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockManager.java',
        ],
        'proj4_part2': [
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockType.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockManager.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockContext.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockUtil.java',
        ],
        'proj4_part3': [
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockType.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockManager.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockContext.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockUtil.java',
            'src/main/java/edu/berkeley/cs186/database/index/LeafNode.java',
            'src/main/java/edu/berkeley/cs186/database/index/InnerNode.java',
            'src/main/java/edu/berkeley/cs186/database/index/BPlusTree.java',
            'src/main/java/edu/berkeley/cs186/database/memory/Page.java',
            'src/main/java/edu/berkeley/cs186/database/table/PageDirectory.java',
            'src/main/java/edu/berkeley/cs186/database/table/Table.java',
            'src/main/java/edu/berkeley/cs186/database/Database.java',
        ],
        'proj5': [
            'src/main/java/edu/berkeley/cs186/database/recovery/ARIESRecoveryManager.java',
            'src/test/java/edu/berkeley/cs186/database/recovery/TestARIESStudent.java',
        ],
    }
    return files[assignment]

def get_path(proj_file):
    index = proj_file.rfind('/')
    if index == -1:
        return ''
    return proj_file[:index]

def get_dirs(proj_files):
    dirs = set()
    for proj in proj_files:
        dirs.add(get_path(proj))
    return dirs

def create_proj_dirs(tempdir, assignment, dirs):
    for d in dirs:
        try:
            tmp_proj_path = tempdir + '/' + assignment + '/' + d
            if not os.path.isdir(tmp_proj_path):
                os.makedirs(tmp_proj_path)
        except OSError:
            print('Error: Creating directory %s failed' % tmp_proj_path)
            exit()
    return tempdir + '/' + assignment

def copy_file(filename, proj_path, tmp_proj_path):
    student_file_path = proj_path + '/' + filename
    tmp_student_file_path = tmp_proj_path + '/' + get_path(filename)
    if not os.path.isfile(student_file_path):
        print('Error: could not find file at %s' % student_file_path)
        exit()
    shutil.copy(student_file_path, tmp_student_file_path)

def create_submission_gpg(student_id, tmp_proj_path):
    # Create submission_info.txt with student id info
    data = {'student_id': student_id}
    txt_submission_path = tmp_proj_path + '/submission_info.txt'
    with open(txt_submission_path, 'w+') as outfile:
        json.dump(data, outfile)

    # Encrypt submission_info.txt to submission_info.gpg
    # and delete submission_info.txt
    public_key_file = os.getcwd() + '/public.key'
    if not os.path.isfile(public_key_file):
        print('Error: Missing the public.key file')
        exit()

    import_cmd = ['gpg', '-q', '--import', 'public.key']
    import_run = subprocess.run(import_cmd)
    import_run.check_returncode()

    gpg_submission_path = tmp_proj_path + '/submission_info.gpg'
    encrypt_cmd = ['gpg', '--output', gpg_submission_path, '--trust-model', 'always', '-e', '-q', '-r', 'CS186 Staff', txt_submission_path]
    encrypt_run = subprocess.run(encrypt_cmd)
    encrypt_run.check_returncode()

    os.remove(txt_submission_path)

def compile_submission(tmp_proj_path, proj_files, assign):
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tempdir:
        os.chdir(tempdir)

        r = subprocess.run(['git', 'init', '-q'])
        r.check_returncode()

        r = subprocess.run(['git', 'remote', 'add', 'local', 'file://' + old_cwd])
        r.check_returncode()

        r = subprocess.run(['git', 'pull', 'local', 'origin/master', '-q'])
        r.check_returncode()

        for filename in proj_files:
            copy_file(filename, tmp_proj_path, tempdir)

        if assign != 'proj1':
            print('Compiling submission...')
            r = subprocess.run(['mvn', 'clean', 'compile', '-q', '-B'], stdout=subprocess.PIPE)

            if r.returncode != 0:
                print('\nError: compilation failed with status', r.returncode, '\n')
                # last 7 lines are not useful output
                print('\n'.join(r.stdout.decode('utf-8').split('\n')[:-7]), '\n')

                os.chdir(old_cwd)
                exit()

            print('Running public tests...')
            r = subprocess.run(['mvn', 'test', '-q', '-B', '-Dproj=' + test_category(assign), '-Ppublic', '-DgenerateReports=false', '-Dsurefire.printSummary=false'], stdout=subprocess.PIPE)

            if r.returncode != 0:
                print('\nWarning: some test failures\n')
                # last 12 lines are not useful output
                print('\n'.join(r.stdout.decode('utf-8').split('\n')[:-12]), '\n')
        else:
            print('Running public tests...')
            r = subprocess.run(['./test.sh'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if r.returncode != 0:
                print('\nWarning: some test failures\n')
                print('\n'.join(r.stdout.decode('utf-8').split('\n')), '\n')

        os.chdir(old_cwd)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='proj submission script')
    parser.add_argument('--student-id', type=check_student_id, help='Berkeley student ID')
    parser.add_argument('--assignment', help='assignment number', choices=PROJ_LIST)
    parser.add_argument('--skip-compile', action='store_true', help='use to skip compilation step')
    args = parser.parse_args()

    if not args.student_id:
        args.student_id = input('Please enter your Berkeley student ID: ')
        check_student_id(args.student_id)

    if not args.assignment:
        args.assignment = input('Please enter the assignment number (one of {}): '.format(str(PROJ_LIST)))
        if args.assignment not in PROJ_LIST:
            print('Error: please make sure you entered a valid assignment number')
            exit()

    with tempfile.TemporaryDirectory() as tempdir:
        proj_files = files_to_copy(args.assignment)
        dirs = get_dirs(proj_files)
        tmp_proj_path = create_proj_dirs(tempdir, args.assignment, dirs)
        for filename in proj_files:
            copy_file(filename, os.getcwd(), tmp_proj_path)

        if not args.skip_compile:
            compile_submission(tmp_proj_path, proj_files, args.assignment)

        create_submission_gpg(args.student_id, tmp_proj_path)

        # Create zip file
        proj_zip_path = os.getcwd() + '/' + args.assignment + '.zip'
        shutil.make_archive(args.assignment, 'zip', tempdir)

        print('Created ' + args.assignment + '.zip')
