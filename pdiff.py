import os, difflib, itertools, pdfplumber, joblib, threading, concurrent.futures, multiprocessing



def output(text):
    lock.acquire()
    print(text)
    lock.release()

def get_pdf_content(fpath):
    pdf = pdfplumber.open(fpath)
    result = str()
    for page in pdf.pages:
        pagecontent = page.extract_text()
        if type(pagecontent) == str:
            result += pagecontent
    pdf.close()
    return result

def get_content(fpath, lock):
    result = ""
    if fpath.lower().endswith(".pdf"):
        result = get_pdf_content(fpath)
    elif fpath.lower().endswith(".py") and fpath != "pdiff.py":
        with open(fpath, encoding="utf8") as f:
            result = f.read()
    else:
        return

    with lock:
        print("Processed:", fpath)

    if len(result) > 250:
        return fpath, result
    else:
        return

def extract_all(fpaths, threads):
    data = []
    print(f"Grabbing PDF content using {threads} threads...")
    m = multiprocessing.Manager()
    lock = m.Lock()

    with concurrent.futures.ProcessPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(get_content, fpath, lock): fpath for fpath in fpaths}
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                data.append(result)
            except Exception as exc:
                print("There was an error. {}".format(exc))
    return data

def compare_all(csplitlist, contents, threads):
    data = []
    print(f"Comparing content using {threads} threads...")
    with concurrent.futures.ProcessPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(compare, clist, contents): clist for clist in csplitlist}
        threads_completed = 0
        for future in concurrent.futures.as_completed(futures):
            try:
                threads_completed += 1
                print(f"Thread {threads_completed} completed")
                result = future.result()
                data.append(result)
            except Exception as exc:
                print("There was an error. {}".format(exc))
    return data

def compare(clist, contents):
    result = []
    for line in clist:
        ratio = difflib.SequenceMatcher(None, contents[line[0]], contents[line[1]]).ratio()
        if ratio > 0.02:
            result.append(str(ratio * 100).replace('.', ",") + ";" + line[0] + ";" + line[1] + "\n")       
    return result

def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))


if __name__ == '__main__':
    fpaths = []
    for path, subdirs, files in os.walk("files"):
        for name in files:
            fpaths.append(os.path.join(path,name))
    
    threads = 16
    
    csv = open("result.csv", "w", encoding="utf-8")

    results = extract_all(fpaths, threads)
    respaths, rescontent = zip(*[x for x in results if x is not None])
    contents = dict(zip(respaths, rescontent))

    #ccount = sum(1 for ignore in itertools.combinations(contents.keys(), 2))

    tlock = threading.Lock()

    clist = []

    for file1, file2 in itertools.combinations(contents.keys(), 2):
        clist.append((file1, file2))

    csplitlist = split(clist, threads)

    cresults = compare_all(csplitlist, contents, threads)

    print("Saving CSV...")
    for chunk in cresults:
        for line in chunk:
            csv.write(line)

    '''for file1, file2 in itertools.combinations(contents.keys(), 2):
        ratio = difflib.SequenceMatcher(None, contents[file1], contents[file2]).ratio()
        if ratio > 0.02:
            csv.write(str(ratio * 100).replace('.', ",") + ";" + file1 + ";" + file2 + "\n")
        counter +=1
        if counter % 1000 == 0:
            print(f"Compared {counter}/{count}")'''

    csv.close()

    print("Done!")
