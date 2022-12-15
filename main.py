import re
import os
import time
import TrophyClass
from pyspark.sql import SparkSession


def find_index_match(indexes, current_index):
    """
        Find presence of given index in list of indexes.

        This function is implementation of binary search without using recursion.

        Parameters
        ----------
        indexes : list
            List of indexes.
        current_index : int
            Index of current block.

        Returns
        -------
        bool
            If the current_index is found in list indexes -> return True.
            If the current_index is not found in list indexes -: return False.
    """
    low = 0
    high = len(indexes)
    mid = 0
    while low <= high:
        mid = (high + low) // 2
        if mid >= len(indexes):
            return False
        if indexes[mid] < current_index:
            low = mid + 1
        elif indexes[mid] > current_index:
            high = mid - 1
        else:
            return True
    return False


def search_in_block(trophies, one_line_block):
    """
        Print result into console.

        This function first checks the category of a block stored in parameter one_line_block.
        If it matches key word award then the results is parsed from this variable using regex expressions.
        The result is stored in variable of class Trophy and is appended into list named trophies.

        Parameters
        ----------
        trophies : list
            Array of trophies to be written into console.
        one_line_block : str
            String which stores the block to be searched

    """
    category = re.search(r"(?<=\bCategory)(\s*:\s*.*?award)", one_line_block, re.IGNORECASE)
    # category match keyword "award" (ignoring casing)
    if category is not None:
        trophy = TrophyClass.Trophy(None, None, None, None, None, None, None, None, None, None, None,
                                    None,
                                    None)
        matchTitle = re.search(r'<title>(.*|\n)</title>', one_line_block)
        if matchTitle is not None:
            if matchTitle.group(1) is not None:
                trophy.title = matchTitle.group(1)

        subHeader = re.search(r"\b(?<=subheader)(\s*=\s*)(.*?\|)", one_line_block)
        if subHeader is not None:
            trophy.subheader = subHeader.group(2)

        awardedFor = re.search(r"\b(?<=awarded_for)(\s*=\s*)(.*?\|)", one_line_block)
        if awardedFor is not None:
            trophy.awarded_for = awardedFor.group(2)

        firstAwarded = re.search(r"\b(?<=firstawarded)(\s*=\s*)(.*?\|)", one_line_block)
        if firstAwarded is not None:
            trophy.first_awarded = firstAwarded.group(2)

        presenter = re.search(r"\b(?<=presenter)(\s*=\s*)(.*?\|)", one_line_block)
        if presenter is not None:
            trophy.presenter = presenter.group(2)

        host = re.search(r"\b(?<=host)(\s*=\s*)(.*?\|)", one_line_block)
        if host is not None:
            trophy.host = host.group(2)

        location = re.search(r"\b(?<=location)(\s*=\s*)(.*?\|)", one_line_block)
        if location is not None:
            trophy.location = location.group(2)

        venue = re.search(r"\b(?<=venue)(\s*=\s*)(.*?\|)", one_line_block)
        if venue is not None:
            trophy.venue = venue.group(2)

        country = re.search(r"\b(?<=country)(\s*=\s*)(.*?\|)", one_line_block)
        if country is not None:
            trophy.country = country.group(2)

        year = re.search(r"\b(?<=year)(\s*=\s*)(.*?})", one_line_block)
        if year is not None:
            trophy.year = year.group(2)

        holder = re.search(r"\b(?<=holder)(\s*=\s*)(.*?\|)", one_line_block)
        if holder is not None:
            trophy.holder = holder.group(2)

        mostNominations = re.search(r"\b(?<=most_nominations)(\s*=\s*)(.*?\|)", one_line_block)
        if mostNominations is not None:
            trophy.most_nominations = mostNominations.group(2)

        reward = re.search(r"\b(?<=reward)(\s*=\s*)(.*?\|)", one_line_block)
        if reward is not None:
            trophy.reward = reward.group(2)

        trophies.append(trophy)


def print_result(trophies):
    """
        Print result into console.

        This function runs in for loop which goes through each object of class Trophy and first checks
        if given parameter of class is not None. If not then the result is printed into console.

        Parameters
        ----------
        trophies : list
            Array of trophies to be written into console.

    """
    for trophy in trophies:
        if trophy.title is not None:
            newTitle = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.title)
            if newTitle is not None and len(newTitle):
                print("Title= " + newTitle)
        if trophy.subheader is not None:
            newSubHeader = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.subheader)
            if newSubHeader is not None and len(newSubHeader):
                print("Description= " + newSubHeader)
        if trophy.awarded_for is not None:
            newAwardedFor = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.awarded_for)
            if newAwardedFor is not None and len(newAwardedFor):
                print("Awarded for= " + newAwardedFor)
        if trophy.fromYear is not None:
            newFromYear = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.fromYear)
            if newFromYear is not None and len(newFromYear):
                print("Awarded in (year)= " + newFromYear)
        if trophy.first_awarded is not None:
            newFirstAwarded = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.first_awarded)
            if newFirstAwarded is not None and len(newFirstAwarded):
                print("First awarded in (year)= " + newFirstAwarded)
        if trophy.most_nominations is not None:
            newMostNominations = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.most_nominations)
            if newMostNominations is not None and len(newMostNominations):
                print("Most nominations= " + newMostNominations)
        if trophy.presenter is not None:
            newPresenter = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.presenter)
            if newPresenter is not None and len(newPresenter):
                print("Presenter= " + newPresenter)
        if trophy.country is not None:
            newCountry = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.country)
            if newCountry is not None and len(newCountry):
                print("Country= " + newCountry)
        if trophy.location is not None:
            newLocation = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.location)
            if newLocation is not None and len(newLocation):
                print("Location= " + newLocation)
        if trophy.venue is not None:
            newVenue = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.venue)
            if newVenue is not None and len(newVenue):
                print("Venue= " + newVenue)
        if trophy.host is not None:
            newHost = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.host)
            if newHost is not None and len(newHost):
                print("Host= " + newHost)
        if trophy.reward is not None:
            newReward = re.sub(r"[^a-zA-Z0-9\s]+", "", trophy.reward)
            if newReward is not None and len(newReward):
                print("Reward= " + newReward)
        print("------------------------------------------")


def write_results_to_file(fileName, trophies, execTime):
    """
        Write result into file with given fileName.

        This function runs in for loop which goes through each object of class Trophy in list trophies
        and first checks if given parameter of class is not None. If not then the result is written into
        file with name fileName_result.txt.

        Parameters
        ----------
        fileName : str
            Name of the file to which will result written.
        execTime : time
            Name of the file to which will be indexes created.
        trophies : list
            Array of trophies to be written to file.

    """
    try:
        os.remove("files/" + fileName + "_result.txt")
    except OSError:
        pass

    file = open("files/" + fileName + "_result.txt", "a", encoding="UTF-8")
    for trophy in trophies:
        if trophy.title is not None:
            file.write("Title= " + trophy.title + "\n")
        if trophy.subheader is not None:
            file.write("Description= " + trophy.subheader + "\n")
        if trophy.awarded_for is not None:
            file.write("Awarded for= " + trophy.awarded_for + "\n")
        if trophy.fromYear is not None:
            file.write("Awarded in (year)= " + trophy.fromYear + "\n")
        if trophy.first_awarded is not None:
            file.write("First awarded in (year)= " + trophy.first_awarded + "\n")
        if trophy.most_nominations is not None:
            file.write("Most nominations= " + trophy.most_nominations + "\n")
        if trophy.presenter is not None:
            file.write("Presenter= " + trophy.presenter + "\n")
        if trophy.country is not None:
            file.write("Country= " + trophy.country + "\n")
        if trophy.location is not None:
            file.write("Location= " + trophy.location + "\n")
        if trophy.venue is not None:
            file.write("Venue= " + trophy.venue + "\n")
        if trophy.host is not None:
            file.write("Host= " + trophy.host + "\n")
        if trophy.reward is not None:
            file.write("Reward= " + trophy.reward + "\n")
        file.write("------------------------------------------\n")

    file.write("Time of execution in seconds: " + str(execTime))


def create_indexes(fileName):
    """
            Perform indexing operation on given fileName.

            This function runs in for loop which goes through each row and is searching for blocks
            that starts with <page> and ends with </page>
            This block will be stored in variable named one_line_block. When the end (</page>) of the block
            is found then the category of this block is checked. If the category meet the requirements then
            the index of current block is written in the file filename_indexes.txt


            Parameters
            ----------
            fileName : str
                Name of the file to which will be indexes created.
    """
    pageStart = False
    pageEnd = False
    pageCounter = 0
    try:
        os.remove("files/" + fileName + "_indexes.txt")
    except OSError:
        pass

    file = open("files/" + fileName + "_indexes.txt", "a", encoding="UTF-8")

    with open("files/" + fileName, "r", encoding="UTF-8") as a_file:
        for line in a_file:
            # looking for page end, found page start
            if pageStart:
                one_line_block += re.sub(r'\r\n|\r|\n', ' ', line)
                matchPageEnd = re.search(r"</page>", line)
                # found page end let's search
                if matchPageEnd is not None:
                    pageEnd = True
                    pageStart = False
            # looking for page start, didn't find page end yet
            else:
                matchPageStart = re.search(r"<page>", line)
                if matchPageStart is not None:
                    pageStart = True
                    one_line_block = ""
                    pageCounter += 1

            # searching in one_line_block string with page content
            if pageEnd:
                category = re.search(r"(?<=\bCategory)(\s*:\s*.*?award)", one_line_block, re.IGNORECASE)
                # category match keyword "award" (ignoring casing)
                if category is not None:
                    file.write(str(pageCounter) + "\n")
                pageEnd = False

    print("Indexy boli vytvorené a zapísané do súboru files/" + fileName + "_indexes.txt")


def search(fileName):
    """
        Perform search operation on given fileName.

        This function runs in for loop which goes through each row and is searching for blocks
        that starts with <page> and ends with </page>
        This block will be stored in variable named one_line_block which will be passed into
        function search_in_block. When the whole file is searched and for loop ends function returns
        array of objects trophies

        Parameters
        ----------
        fileName : str
            Name of the file to be searched.

        Returns
        -------
        list
            list of objects of class Trophy
    """
    pageStart = False
    pageEnd = False
    trophies = []
    pageCounter = 0
    indexes = []

    if os.path.exists("files/" + fileName + "_indexes.txt"):
        with open("files/" + fileName + "_indexes.txt", "r") as f:
            for line in f:
                indexes.append(int(line.strip()))

    # for fileName in fileNames:
    with open("files/" + fileName, "r", encoding="UTF-8") as a_file:
        for line in a_file:
            # looking for page end, found page start
            if pageStart:
                if len(indexes):
                    if not find_index_match(indexes, pageCounter) and pageEnd is not True:
                        pageStart = False
                        pageEnd = False
                    else:
                        one_line_block += re.sub(r'\r\n|\r|\n', ' ', line)
                        matchPageEnd = re.search(r"</page>", line)
                        # found page end let's search
                        if matchPageEnd is not None:
                            pageEnd = True
                            pageStart = False
                else:
                    one_line_block += re.sub(r'\r\n|\r|\n', ' ', line)
                    matchPageEnd = re.search(r"</page>", line)
                    # found page end let's search
                    if matchPageEnd is not None:
                        pageEnd = True
                        pageStart = False
            # looking for page start, didn't find page end yet
            else:
                matchPageStart = re.search(r"<page>", line)
                if matchPageStart is not None:
                    pageStart = True
                    one_line_block = ""
                    pageCounter += 1

            # searching in one_line_block string with page content
            if pageEnd:
                search_in_block(trophies, one_line_block)
                pageEnd = False
    return trophies


def main():
    """
        Main function which is called when the program starts.
    """
    # change this to names of the files to be searched or indexed
    fileNames = ["trophies.xml"]

    spark = SparkSession.builder.master("local").appName("VINF").getOrCreate()

    while 1:
        print("-----------------------------------------------------")
        print("| Vyberte akciu ktorú chcete vykonať:               |")
        print("| 1.) Zindexovať dataset a uložiť indexy do súboru. |")
        print("| 2.) Vyhľadať výsledky.                            |")
        print("| 3.) Ukončiť.                                      |")
        print("-----------------------------------------------------")
        option = input("Váš výber: ")

        if option == "1":
            rdd = spark.sparkContext.parallelize(fileNames)
            rdd.map(create_indexes).collect()
        elif option == "2":
            startTime = time.time()
            rdd = spark.sparkContext.parallelize(fileNames)
            arr = rdd.map(search).collect()
            for trophies in arr:
                print_result(trophies)
            endTime = time.time()
            print("Time of execution in seconds: ", endTime - startTime)
        elif option == "3":
            break
        else:
            print("Zle zadaná možnosť")


if __name__ == "__main__":
    main()
