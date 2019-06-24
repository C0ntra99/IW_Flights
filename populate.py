import psycopg2
import uuid
import sys
import math

def connect_database():
     
    conn = psycopg2.connect("dbname='postgres' user='candidate4783'")
    return conn
    


##When inserting data make sure the data does not already exist

## Each function should return the primary key of whatever was inserted

def insert_airline_dim(data):
    def already_exists(airline_code):
        t = (airline_code, )
        cur.execute("SELECT airline_ID FROM candidate4783.airline_dim WHERE airlinecode=%s", t)
        if cur.fetchone():
            return False
        else:
            return True
    if already_exists(data['AIRLINECODE']):
        data["airline_id"] = str(uuid.uuid4()).replace("-","")
        sql = "INSERT INTO candidate4783.airline_dim({})".format(",".join(data.keys()))
        sql += " VALUES ('{}')".format("','".join(map(str, data.values())))
        sql += ";"
        cur.execute(sql)
        print("Executing SQL: {}".format(sql))
        conn.commit()
    else:
        pass

def insert_airplane_dim(data):
    def already_exists(tailnum):
        t = (tailnum, )
        cur.execute("SELECT airplane_id FROM candidate4783.airplane_dim WHERE tailnum=%s", t)
        if cur.fetchone():
            return False
        else:
            return True
    if already_exists(data["TAILNUM"]):
        data["airplane_id"] = str(uuid.uuid4()).replace("-","")
        data["AIRLINENAME"] = "( SELECT AIRLINENAME from candidate4783.airline_dim WHERE AIRLINECODE='{}')".format(data["AIRLINENAME"])
        sql = "INSERT INTO candidate4783.airplane_dim({})".format(",".join(data.keys()))
        sql += " VALUES ("+data["AIRLINENAME"]+","
        del data['AIRLINENAME']
        sql+="'{}')".format("','".join(map(str, data.values())))
        sql += ";"
        #print(sql)
        cur.execute(sql)
        print("Executing SQL: {}".format(sql))
        conn.commit()
    else:
        pass
    #

def insert_airport_dim(data):
    def already_exists(airport_code):
        t = (airport_code, )
        cur.execute("SELECT airport_id FROM candidate4783.airport_dim WHERE airportcode=%s", t)
        if cur.fetchone():
            return False
        else:
            return True
    if already_exists(data['AIRPORTCODE']):
        data["airport_id"] = str(uuid.uuid4()).replace("-","")
        sql = "INSERT INTO candidate4783.airport_dim({})".format(",".join(data.keys()))
        sql += " VALUES ('{}')".format("','".join(map(str, data.values())))
        sql += ";"
        cur.execute(sql)
        conn.commit()
    else:  
        pass

def insert_flight_data(data):
   
    def already_exists(TransactionID):
        t = (TransactionID, )
        cur.execute("SELECT TransactionID FROM candidate4783.flight_data WHERE TransactionID=%s", t)
        if cur.fetchone():
            return False
        else:
            return True
    if already_exists(data['TransactionID']): 
        data["flight_id"] = str(uuid.uuid4()).replace("-","")
        data['AIRLINE'] = "( SELECT airlinename FROM candidate4783.airline_dim WHERE airlinecode='{}')".format(data["AIRLINE"])
        data['ORIGINAIRPORT'] = "(SELECT airportname FROM candidate4783.airport_dim WHERE airportcode='{}')".format(data["ORIGINAIRPORT"])
        data['DESTAIRPORT'] = "( SELECT airportname FROM candidate4783.airport_dim WHERE airportcode='{}')".format(data["DESTAIRPORT"])
        data['TAILNUM'] = "( SELECT tailnum FROM candidate4783.airplane_dim WHERE tailnum='{}')".format(data['TAILNUM'])
        
        sql = "INSERT INTO candidate4783.flight_data({})".format(",".join(data.keys()))
        sql += " VALUES ("+data["AIRLINE"]+","+data["TAILNUM"]+","+data["ORIGINAIRPORT"]+","+data["DESTAIRPORT"]+","
        #del data['AIRLINE']
        #del data['ORIGINAIRPORT']
        #del data['DESTAIRPORT']
        #del data['TAILNUM']
        sql+="'{}')".format("','".join(list(map(str, data.values()))[4:]))

        cur.execute(sql)
        print("Executing SQL: {}".format(sql))
        conn.commit()
    else:
        pass

def start_airport_data(data):
    new_data = {}
    new_data['AIRPORTNAME'] = data['DESTAIRPORTNAME'].split(":")[1].replace("'", '')
    new_data['AIRPORTCODE'] = data['DESTAIRPORTCODE']
    new_data['CITYNAME'] = data['DESTCITYNAME'].replace("'", '')
    new_data['STATENAME'] = data['DESTSTATENAME'].replace("'", '')
    insert_airport_dim(new_data)

def start_airline_data(data):
    new_data = {}
    new_data['AIRLINENAME'] = data['AIRLINENAME'].split(":")[0]
    new_data['AIRLINECODE'] = data['AIRLINECODE']
    insert_airline_dim(new_data)

def start_airplane_data(data):
    new_data = {}
    new_data['AIRLINENAME'] = data["AIRLINECODE"]
    new_data['TAILNUM'] = data["TAILNUM"].replace("'", '')
    insert_airplane_dim(new_data)

def return_distance_group(num):
    def roundup(num):
        return int(math.ceil(num / 100.0)) * 100
    def rounddown(num):
        return int(math.floor(num / 100.0)) * 100
    return "{}-{}".format(rounddown(num)+1 if rounddown(num) != 0 else rounddown(num), roundup(num))

def start_flight_data(data):
    #data = map(, data)
    #print(data)
    new_data = {}
    ## AirlineCode_fk
    new_data['AIRLINE'] = data['AIRLINECODE']
    ## AirplaneTailNum_fk
    new_data['TAILNUM'] = data['TAILNUM'].replace("'", '')
    
    ## OriginAirport_fk
    new_data['ORIGINAIRPORT'] = data['ORIGINAIRPORTCODE']
    ## DestAirport_fk
    new_data['DESTAIRPORT'] = data['DESTAIRPORTCODE']

    new_data['TransactionID'] = data['TRANSACTIONID']

    ##Needs to be parsed and a datetime object created
    new_data['Flight_Date'] = data['FLIGHTDATE']
    new_data['CRSDepartureTime'] = data['CRSDEPTIME']
    new_data['DepartureTime'] = data['DEPTIME']
    new_data['DepartureDelay'] = data['DEPDELAY']
    new_data['TaxiOut'] = data['TAXIOUT']
    new_data['WheelsOff'] = data['WHEELSOFF']
    new_data['WheelsOn'] = data['WHEELSON']
    new_data['TaxiIn'] = data['TAXIIN']
    new_data['CRSArrivalTime'] = data['CRSARRTIME']
    new_data['ArrivalTime'] = data['ARRTIME']
    new_data['ArrivalDelay'] = data['ARRDELAY']
    new_data['CRSElapsedTime'] = data['CRSELAPSEDTIME']
    new_data['ActualElapsedTime'] = data['ACTUALELAPSEDTIME']
    new_data['Cancelled'] = data['CANCELLED']
    new_data['Diverted'] = data['DIVERTED']
    new_data['FlightNumber'] = data['FLIGHTNUM']
    ## create distance groups
    number = int(str(data['DISTANCE\n']).strip().split(' ')[0])
    new_data['DistanceMiles'] = return_distance_group(number)
    
    ## determine if the depature is longer than 15 minutes
    #print(data['DEPDELAY'])
    if data['DEPDELAY']:
        new_data['DepatureDelayLong'] = True if int(data["DEPDELAY"]) < -15.0 else False
    else:
        new_data['DepatureDelayLong'] = False
    
    insert_flight_data(new_data)
if __name__ == "__main__":
    try:
        
        conn = connect_database()
        cur = conn.cursor()


        flight_data_file = open("Data/flights_NEW.txt", "r")
        flight_data_array= flight_data_file.readlines()
        flight_data_cols = flight_data_array[0].split("|")

        #print(insert_airline_dim({"airline_code":"WN","airline_name":"Southwest Airlines Ce.: WN"}))
        
        for flight in flight_data_array[1:]:
            start_airport_data(dict(zip(flight_data_cols, flight.split("|"))))
           #pass
            #start_airline_data(dict(zip(flight_data_cols, flight.split("|"))))
            
            start_airplane_data(dict(zip(flight_data_cols, flight.split("|"))))
            start_flight_data((dict(zip(flight_data_cols, flight.split("|")))))
            #break
                #print(data)
                #start_airport_data(data)
                #pass
        #conn.commit()
    except KeyboardInterrupt:
        flight_data_file.close()
        


