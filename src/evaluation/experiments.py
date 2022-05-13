# Create Updates and Update Revisions from the BEAR B Benchmark -> instant data
# These Updates have different time interval -> wide, small, much overlap, less overlap -> normal distribution
# retrospective or future -> nearby to its creation date or far away from it
# Different amount of Updates -> [21046, 10523, 5261, 2630, 1315]
# Create after a day a Tag and Tag Revision to indicate the new version -> which we should query
# Single snapshot middle transaction time, and much overlap in valid time and not a lot of overlap in valid time
# Branches
# Modifications in valid time

def experiment1():
    pass

if __name__ == '__main__':
    file_name = '/Users/lisameijer/Universiteit/ThesisComputerScience/data/alldata.CB.nt-2/data-added_1-2.nt'
    with open(file_name) as f:
        lines = f.readlines()

    count = 0
    for line in lines:
        count += 1
        print(f'line {count}: {line}')

    #data = open('test.ttl').read()
    # headers = {'Content-Type': 'text/turtle;charset=utf-8'}
    # r = requests.post('http://localhost:3030/mydataset/data?default', data=data, headers=headers)