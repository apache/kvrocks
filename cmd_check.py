import pandas as pd
import os 
from glob import glob
import re
import sys
import subprocess

# copy from x.py
def run(args, msg=None, **kwargs):
    sys.stdout.flush()
    p = subprocess.Popen(args, **kwargs)
    code = p.wait()
    if code != 0:
        err = f"""
failed to run: {args}
exit with code: {code}
error message: {msg}
"""
        raise RuntimeError(err)
    else:
        return p.stdout
def GetSortedTagList():
    output = run(["git", "tag", "-l", "--sort=v:refname"], stdout=subprocess.PIPE)
    git_tag_str = output.read().decode().strip()
    git_tag_list = git_tag_str.split('\n')
    git_tag_list_final = git_tag_list[1:]  # remove v.1.1.2
    # print(git_tag_list_final)
    return git_tag_list_final

def GetAllCommand():
    # if network error happen, you can download the html file and then use local file.
    tables = pd.read_html('https://kvrocks.apache.org/docs/supported-commands/') 
    # tables = pd.read_html("./support_cmd.html")  
    CommandsList = []
    for tbl in tables:
        Commands = []
        for index, row in tbl.iterrows():
            Commands.append(row.iloc[0])
        CommandsList.append(Commands)
    # print(CommandsList)

    CommandsLowerList = []
    for Commands in CommandsList:
        for Command in Commands:
            # fix err cmd
            if Command.lower() == "getsearchstore":
                CommandsLowerList.append("geosearchstore")
            elif Command.lower() == "bf.mexist":
                CommandsLowerList.append("bf.mexists")
            else:
                CommandsLowerList.append(Command.lower())      
    return CommandsLowerList

# search range
def GetPattern(LowerCommand, git_tag_version):
    pattern = ""
    git_version_list = git_tag_version.split('.')
    if git_version_list[0] == "v2":
        if int(git_version_list[1]) >= 2:
            pattern = 'MakeCmdAttr\\<.*\\>\\(\"' + LowerCommand + '\",'
        else:
            #v2.1.* or v2.0.*
            pattern = 'ADD_CMD\\(\"' + LowerCommand + '\",'
    elif git_version_list[0] == "v1":
        # v1.0.*
        if int(git_version_list[1]) == 0:
            pattern = '{\"' + LowerCommand + '\",'
        # v1.1.*
        elif int(git_version_list[1]) == 1:
            # less than v1.1.11
            if int(git_version_list[2]) <= 11:
                pattern = '{\"' + LowerCommand + '\",'
            else:
                pattern = 'ADD_CMD\\(\"' + LowerCommand + '\",'
        elif int(git_version_list[1]) == 2:
            pattern = 'ADD_CMD\\(\"' + LowerCommand + '\",'
        elif int(git_version_list[1]) == 3:
            pattern = 'ADD_CMD\\(\"' + LowerCommand + '\",'
    return pattern
    
def Main():

    git_tag_list = GetSortedTagList()
    CommandsLowerList = GetAllCommand()
    CommandsSupportVersion = ["-"] * len(CommandsLowerList)

    for git_tag in git_tag_list:
        print("kvrocks git tag: ", git_tag)
        output = run(["git", "checkout", git_tag], stdout=subprocess.PIPE)
        print("now branch: ", output.read().decode().strip())

        target = os.getcwd()
        subpath = '/src/**/'
        if git_tag == 'v1.0.0':
            subpath = '/kvrocks/src/**/'
        files = glob(target + subpath + '*.cc', recursive=True)
        for LowerCommandIdx in range(len(CommandsLowerList)):
            LowerCommand = CommandsLowerList[LowerCommandIdx]
            pattern = GetPattern(LowerCommand, git_tag)
            if pattern == "":
                print("get pattern err")
                return
            if CommandsSupportVersion[LowerCommandIdx] != "-": # it means found min support version
                continue
            for file in files:
                with open(file,'r') as f:
                    ff=f.read()
                    matchObj = re.search(pattern=pattern, string=ff)
                    if matchObj:
                        CommandsSupportVersion[LowerCommandIdx] = git_tag

    CommandsUpperList = []
    for CommandsLower in CommandsLowerList:
        CommandsUpperList.append(CommandsLower.upper())               
    csvDict = {}
    csvDict["Command"] = CommandsUpperList
    csvDict["MinSupportVersion"] = CommandsSupportVersion
    resDf = pd.DataFrame(csvDict)
    resDf.to_csv('./support_version_res.csv')
if __name__ == '__main__':
    Main()