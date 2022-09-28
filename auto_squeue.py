import datetime
import time
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import subprocess

# (1) Google Spread Sheetsにアクセス
def connect_gspread(token_file_path,key):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(token_file_path, scope)
    gc = gspread.authorize(credentials)
    SPREADSHEET_KEY = key
    worksheet = gc.open_by_key(SPREADSHEET_KEY).sheet1
    return worksheet

token_file_path = "token.json"
spread_sheet_key = open('spread_sheet_key').read()
ws = connect_gspread(token_file_path, spread_sheet_key)

class Job:
    def __init__(self, 
            jobid: int = -1, partition: str = '', name: str = '', user: str = '', 
            status: str = '', time: str = '', nodes: int = -1, nodelist: str = '') -> None:
        self.jobid: int = jobid
        self.partition: str = partition
        self.name: str = name
        self.user: str = user
        self.status: str = status
        self.time: str = time
        self.nodes: int = nodes
        self.nodelist: str = nodelist
    def __str__(self) -> str:
        return str(
            {
                'jobid': self.jobid,
                'partition': self.partition,
                'name': self.name,
                'user': self.user,
                'status': self.status,
                'time': self.time,
                'nodes': self.nodes,
                'nodelist': self.nodelist,
            }
        )
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(' + \
            f'{self.jobid}, ' + \
            f'"{self.partition}", ' + \
            f'"{self.name}", ' + \
            f'"{self.user}", ' + \
            f'"{self.status}", ' + \
            f'"{self.time}", ' + \
            f'{self.nodes}, ' + \
            f'"{self.nodelist}")'

def find_first_back(target: str, word: str) -> int:
    pos = target.find(word)
    if pos != -1:
        return pos + len(word) - 1
    else:
        return -1
    
def delete_spaces(text: str) -> str:
    while text[-1] == ' ': text = text[:-1]
    while text[0] == ' ': text = text[1:]
    return text

def load_squeue(squeue: str) -> list[Job]:
    lines = list(filter(lambda l: len(l) > 0, squeue.split('\n')))
    jobs = [Job() for line in lines[1:]]
    jobid_pos = find_first_back(lines[0], 'JOBID')
    partition_pos = find_first_back(lines[0], 'PARTITION')
    name_pos = find_first_back(lines[0], 'NAME')
    user_pos = find_first_back(lines[0], 'USER')
    status_pos = find_first_back(lines[0], 'ST')
    time_pos = find_first_back(lines[0], 'TIME')
    nodes_pos = find_first_back(lines[0], 'NODES')
    for i, line in enumerate(lines[1:]):
        jobs[i].jobid = int(delete_spaces(line[: jobid_pos + 1]))
        jobs[i].partition = delete_spaces(line[jobid_pos + 1: partition_pos + 1])
        jobs[i].name = delete_spaces(line[partition_pos + 1: name_pos + 1])
        jobs[i].user = delete_spaces(line[name_pos + 1: user_pos + 1])
        jobs[i].status = delete_spaces(line[user_pos + 1: status_pos + 1])
        jobs[i].time = delete_spaces(line[status_pos + 1: time_pos + 1])
        jobs[i].nodes = int(delete_spaces(line[time_pos + 1: nodes_pos + 1]))
        jobs[i].nodelist = delete_spaces(line[nodes_pos + 1:])
    return jobs

def update_sheet() -> None:
    MAX_JOBS = 32
    date_now = datetime.datetime.now(pytz.timezone('Asia/Tokyo'))
    ws.update_cell(1, 2, date_now.year)
    ws.update_cell(1, 3, date_now.month)
    ws.update_cell(1, 4, date_now.day)
    ws.update_cell(1, 5, date_now.hour)
    ws.update_cell(1, 6, date_now.minute)
    jobs = load_squeue(subprocess.check_output('squeue').decode())
    
    ds: list[gspread.cell.Cell] = ws.range(2, 2, 2 + MAX_JOBS, 9)
    for i, label in enumerate(['JOBID', 'PARTITION', 'NAME', 'USER', 'ST', 'TIME', 'NODES', 'NODELIST(REASON)']):
        ds[i].value = label
    for i in range(8, 8 * (1 + MAX_JOBS)): ds[i].value = ''
    for i, job in enumerate(jobs):
        ds[(i + 1) * 8].value = jobs[i].jobid
        ds[(i + 1) * 8 + 1].value = jobs[i].partition
        ds[(i + 1) * 8 + 2].value = jobs[i].name
        ds[(i + 1) * 8 + 3].value = jobs[i].user
        ds[(i + 1) * 8 + 4].value = jobs[i].status
        ds[(i + 1) * 8 + 5].value = jobs[i].time
        ds[(i + 1) * 8 + 6].value = jobs[i].nodes
        ds[(i + 1) * 8 + 7].value = jobs[i].nodelist
    ws.update_cells(ds)
    
while True:
    date_now = datetime.datetime.now(pytz.timezone('Asia/Tokyo'))
    if date_now.second == 0:
        try:
            update_sheet()
        except Exception as e:
            print(e)
            print(f'Failed to update sheet at {date_now}')
        time.sleep(30)
    else:
        time.sleep(min(max(60 - date_now.second - 1, 0.5), 60))