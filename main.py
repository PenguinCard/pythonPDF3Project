# pdf 읽고 병합용
from PyPDF2 import PdfFileReader, PdfFileWriter

from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.pagesizes import letter

# pdf 텍스트 추출
import pdfplumber
# excel 데이터 추출
from openpyxl import load_workbook
# html 데이터 추출
from bs4 import BeautifulSoup

import os
import re
import io
import math
import time

# spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row


def main():
    start_time = time.time()

    # 스파크 세션 생성
    spark = SparkSession.builder.config('spark.driver.host', '127.0.0.1').getOrCreate()
    # 현재경로 가져오기
    current_path = os.getcwd()
    # 파일리스트 가져오기
    files = os.listdir()

    # 폰트 설정
    pdfmetrics.registerFont(TTFont("D2Coding", "D2Coding.ttf"))

    # 송장감지 텍스트
    regex = re.compile(r'LZDID|First Mile Warehouse')

    # 생성할 spark View Schema 정의
    Schema = StructType([
        StructField("orderNumber", StringType(), True),
        StructField("orderSku", StringType(), True)
    ])
    # spark 에 구성될 행 데이터
    rows = []

    idx = 2

    # xlsx 확장자를 가지는 파일 가져오기
    xlsx_files = list(filter(lambda s: True if s.find('.xlsx') >= 0 else False, files))
    # pdf 확장자를 가지는 파일 가져오기
    pdf_files = list(filter(lambda s: True if s.find('.pdf') >= 0 else False, files))

    # xlsx 파일 갯수만큼 반복
    for xlsx_file in xlsx_files:
        print(current_path, xlsx_file)
        wb = load_workbook("{}/{}".format(current_path, xlsx_file))
        sheet = wb["sheet1"]
        # M열과 F열의 데이터가 둘 중에 하나라도 없으면 break
        while sheet["M{}".format(idx)].value is not None and sheet["F{}".format(idx)].value is not None:
            # 주문번호
            order_number = str(sheet["M{}".format(idx)].value)
            # text 정렬작업
            text = str(sheet["F{}".format(idx)].value)
            text = re.sub(r'\)\s1', ')', text)
            text = re.sub(r'\(\d{4}\)|[+]', ',', text)
            text = text.strip()
            texts = text.split(',')
            texts = list(filter(None, texts))
            texts = list(map(lambda s: s.strip(), texts))

            # spark View에 행 추가
            for text in texts:
                rows.append(
                    Row(
                        order_number,
                        text
                    )
                )

            idx = idx + 1
    create_view_time = time.time()
    # rows와 Schema를 이용해 dataframe 생성 후 View("ORDERSKU") 로 전환
    spark.createDataFrame(rows, Schema).createOrReplaceTempView("ORDERSKU")
    # spark로 json 파일을 읽은 뒤 SQL View(이름은 'SKU')로 생성
    spark.read.json('./SKU.json', multiLine=True).createOrReplaceTempView("SKU")
    print('Create View Time : {} sec'.format(time.time() - create_view_time))

    for pdf_file in pdf_files:
        baseName = re.sub(r"\.pdf", "", pdf_file)

        # html 파일을 읽어온 뒤 태그로 추출할 수 있는 형태로 변환
        html = open('{}/{}.html'.format(current_path, baseName), 'rt', encoding='utf-8').read()
        dom = BeautifulSoup(html, 'html.parser')

        # html 파일에서 order_numbers 담는 리스트
        order_numbers = []

        # order_number 이 있는 셀렉터
        contents = dom.select('table tbody tr td p')
        # 셀렉터 반복하여 order_number 추출
        for content in contents:
            text = re.findall(r'Order Number[:] \d+', content.text)
            if len(text) > 0:
                order_number = re.sub(r'Order Number[:]', '', text[0])
                order_numbers.append(order_number.strip())

        # 변수 0으로 초기화
        idx = 0

        # pyPDF2를 통해 pdf를 읽어옴(merge용)
        exist_pdf = PdfFileReader('{}/{}.pdf'.format(current_path, baseName))
        # pdf 작성
        output = PdfFileWriter()

        pages = pdfplumber.open('{}/{}.pdf'.format(current_path, baseName)).pages

        for pageNum, page in enumerate(pages):
            # 텍스트를 추출하여 송장 감지
            texts = page.extract_text()
            check_invoice = regex.search(texts)

            if check_invoice is not None:
                query_order = "SELECT orderSku FROM ORDERSKU WHERE orderNumber like '{}'".format(order_numbers[idx])
                df = spark.sql(query_order)
                products = {}
                for row in df.rdd.collect():
                    query_sku = "SELECT * FROM SKU WHERE NAME like '%{}%'".format(row.orderSku)
                    df2 = spark.sql(query_sku)
                    try:
                        print(df2.rdd.take(1))
                        row_data = df2.rdd.take(1)[0]
                        data_text = "{} ({})".format(row_data.NAME, row_data.CODE)
                        if data_text in products:
                            products[data_text] = products[data_text] + 1
                        else:
                            products[data_text] = 1
                    except:
                        continue

                packet = io.BytesIO()
                can = canvas.Canvas(packet, pagesize=letter)  # pdf 출력 letter 사이즈
                can.setFillColorRGB(255, 255, 255)  # 캔버스 색상 흰 색
                can.setLineWidth(0.75)  # 라인 너비
                can.rect(6.45, 525, 272.3, 70, fill=1)  # 사각형 생성
                can.setFillColorRGB(0, 0, 0)  # 캔버스 색상 검정색

                i = 0  # 딕셔너리 index
                for key, value in products.items():
                    pos_y = 587.5 - (11 * math.floor(i / 3))  # text y축
                    pos_x = 8  # text x축
                    if i % 3 == 0:
                        pos_x = 8
                    elif i % 3 == 1:
                        pos_x = 100
                    else:
                        pos_x = 192

                    write_text = '{} {}'.format(key, value)
                    if len(write_text) < 15:
                        can.setFont("D2Coding", 8)  # 폰트종류: D2Coding, 폰트크기: 8
                    elif len(write_text) >= 15 & len(write_text) < 17:
                        can.setFont("D2Coding", 7)  # 폰트종류: D2Coding, 폰트크기: 7
                    elif len(write_text) >= 17 & len(write_text) < 19:
                        can.setFont("D2Coding", 6)  # 폰트종류: D2Coding, 폰트크기: 6
                    else:
                        can.setFont("D2Coding", 5)  # 폰트종류: D2Coding, 폰트크기: 5

                    can.drawString(pos_x, pos_y, write_text)
                    i = i + 1

                can.save()
                packet.seek(0)
                new_pdf = PdfFileReader(packet)
                invoice_page = exist_pdf.getPage(pageNum)
                invoice_page.mergePage(new_pdf.getPage(0))
                output.addPage(invoice_page)

                idx = idx + 1

        with open("{}/_{}.pdf".format(current_path, baseName), "wb") as outputStream:
            output.write(outputStream)

    print('total work time : {} sec'.format(time.time() - start_time))


if __name__ == "__main__":
    main()
