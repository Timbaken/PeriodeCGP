# PeriodeCGP

Belangrijke dingen om op orde te hebben voor gebruik:
 - Het hebben van een database met de naam 'huwebshop'
   - de collecties binnen deze database heten:
     - products
     - sessions
     - profiles
 - Het hebben van:
   - python
   - mongoDB
   - SQL
   - Flask
 - Vul de naam van uw database en uw wachtwoord in op deze plekken:
   - sprint_2 > winkelmandje > regel 6
   - sprint_3 > categoriepagina > regel 4
   - sprint_3 > productpagina > regel 5
 - Je kunt het aantal recommendations dat je wilt hebben op een pagina makkelijk aanpassen op:
   - huw.py > regel 23
 
Nu je dit hebt gedaan kun je deze commandos uitvoeren in de command prompt:
set FLASK_APP=huw_recommend.py
python -m flask run --port 5001

Laat deze command prompt ook runnen tijdens uw gebruik

Als het goed is zal er dit staan:
* Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)

Ga naar deze website toe om gebruik te maken van de website:
http://127.0.0.1:5000

Als je wilt stoppen:
klik CTRL+C in de command prompt
