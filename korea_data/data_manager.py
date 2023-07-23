import pymysql

con = pymysql.connect(user='root',
                      passwd='kkljjh',
                      host='localhost',
                      db='data',
                      charset='utf8')

cursor = con.cursor()

query = f"""
    insert into kor_sector (IDX_CD, CMP_CD, CMP_KOR, SEC_NM_KOR, 기준일)
    values (%s,%s,%s,%s,%s) as new
    on duplicate key update
    IDX_CD = new.IDX_CD, CMP_KOR = new.CMP_KOR, SEC_NM_KOR = new.SEC_NM_KOR
"""

args = kor_sector.values.tolist()
cursor.executemany(query, args)
con.commit()

con.close()