import pymongo
import csv
import time


def mark(string):
    if string != 'null' and string is not None:
        return float(string.replace(',', '.'))

ZNO2019 = 'Odata2019File.csv'
ZNO2020 = 'Odata2020File.csv'


tries = 5
while tries:
    try:
        db_client = pymongo.MongoClient('mongodb://admin:password@mongodb')

        db = db_client['znodata']
        collection = db['zno']

        start = time.time()
        
        count = collection.count_documents({'year': 2019})
        print(count)

        with open(ZNO2019, 'r', encoding='windows-1251') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            headers = next(reader)

            outid = headers.index('OUTID')
            birth = headers.index('Birth')
            sextypename = headers.index('SEXTYPENAME')
            regname = headers.index('REGNAME')
            matTest = headers.index('matTest')
            matBall100 = headers.index('matBall100')
            matTestStatus = headers.index('matTestStatus')
            year = 2019

            for idx in range(count):
                if idx % 10000 == 0:
                    print(f'{idx} records skipped!')
                
                next(reader)
            
            for idx, row in enumerate(reader):
                values = {'outID': row[outid],
                          'birth': row[birth],
                          'sexTypeName': row[sextypename],
                          'regName': row[regname],
                          'testName': row[engTest],
                          'testMark': mark(row[engBall100]),
                          'testStatus': row[engTestStatus] != 'null',
                          'year': year}
                
                collection.insert_one(values)

                if idx % 10000 == 0:
                    print(f'{idx} records added!')


        count = collection.count_documents({'year': 2021})
        print(count)
    
        with open(ZNO2020, 'r', encoding='utf-8-sig') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            headers = next(reader)

            outid = headers.index('OUTID')
            birth = headers.index('Birth')
            sextypename = headers.index('SexTypeName')
            regname = headers.index('RegName')
            engTest = headers.index('EngTest')
            engBall100 = headers.index('EngBall100')
            engTestStatus = headers.index('EngTestStatus')
            year = 202

            for idx in range(count):
                if idx % 10000 == 0:
                    print(f'{idx} records skipped!')

                next(reader)
            
            for idx, row in enumerate(reader):
                values = (row[outid], row[birth], row[sextypename], row[regname], mark(row[matBall100]), row[matTestStatus] != 'null', 2021)
                values = {'outID': row[outid],
                          'birth': row[birth],
                          'sexTypeName': row[sextypename],
                          'regName': row[regname],
                          'testName': row[matTest],
                          'testMark': mark(row[matBall100]),
                          'testStatus': row[matTestStatus] != 'null',
                          'year': year}
                
                collection.insert_one(values)

                if idx % 10000 == 0:
                    print(f'{idx} records added!')
        
        print('All data successfuly inserted')

        with open('execution time.txt', 'w') as timefile:
            timefile.write(f'Execution time: {time.time() - start}')
            print(f'Execution time: {time.time() - start}')

        
        pipeline = [
            {'$match': {'testStatus': True}},
            {'$group': {
                '_id': {
                    'regName': '$regName',
                    'year': '$year'
                },
                'avgMark': {'$avg': '$testMark'}
            }},
            {'$sort': {'_id.regName': 1}}
        ]
        result = collection.aggregate(pipeline)


        with open('ZNOdata.csv', 'w', newline='', encoding='utf-8') as csvfile:
            avgMarks2019 = []
            avgMarks2021 = []
            for el in result:
                regName = el['_id']['regName'] 
                year = el['_id']['year']
                avgMark = el['avgMark']

                if year == 2019:
                    avgMarks2019.append((regName, avgMark))
                elif year == 2021:
                    avgMarks2021.append((regName, avgMark))

            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(['regName', 'avgMark2019', 'avgMark2020'])
            
            for i in range(len(avgMarks2019)):
                writer.writerow([avgMarks2019[i][0], avgMarks2019[i][1], avgMarks2020[i][1]])
        
        print('Created file ZNOdata.csv with statistics')
        
        tries = 0

    except FileNotFoundError as err:
        tries = 0
        # print('FileNotFoundError')
        print(f'File {err.filename} does not exist!')

    except:
        print('Undefined error!!')
