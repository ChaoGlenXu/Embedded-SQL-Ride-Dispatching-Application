"""
Part2 of csc343 A2: Code that could be part of a ride-sharing application.
csc343, Fall 2022
University of Toronto

--------------------------------------------------------------------------------
This file is Copyright (c) 2022 Diane Horton and Marina Tawfik.
All forms of distribution, whether as given or with any changes, are
expressly prohibited.
--------------------------------------------------------------------------------
"""
import psycopg2 as pg
import psycopg2.extensions as pg_ext
from typing import Optional, List, Any
from datetime import datetime
import re
import math

class GeoLoc:
    """A geographic location.

    === Instance Attributes ===
    longitude: the angular distance of this GeoLoc, east or west of the prime
        meridian.
    latitude: the angular distance of this GeoLoc, north or south of the
        Earth's equator.

    === Representation Invariants ===
    - longitude is in the closed interval [-180.0, 180.0]
    - latitude is in the closed interval [-90.0, 90.0]

    >>> where = GeoLoc(-25.0, 50.0)
    >>> where.longitude
    -25.0
    >>> where.latitude
    50.0
    """
    longitude: float
    latitude: float

    def __init__(self, longitude: float, latitude: float) -> None:
        """Initialize this geographic location with longitude <longitude> and
        latitude <latitude>.
        """
        self.longitude = longitude
        self.latitude = latitude

        assert -180.0 <= longitude <= 180.0, \
            f"Invalid value for longitude: {longitude}"
        assert -90.0 <= latitude <= 90.0, \
            f"Invalid value for latitude: {latitude}"


class Assignment2:
    """A class that can work with data conforming to the schema in schema.ddl.

    === Instance Attributes ===
    connection: connection to a PostgreSQL database of ride-sharing information.

    Representation invariants:
    - The database to which connection is established conforms to the schema
      in schema.ddl.
    """
    connection: Optional[pg_ext.connection]

    def __init__(self) -> None:
        """Initialize this Assignment2 instance, with no database connection
        yet.
        """
        self.connection = None

    def connect(self, dbname: str, username: str, password: str) -> bool:
        """Establish a connection to the database <dbname> using the
        username <username> and password <password>, and assign it to the
        instance attribute <connection>. In addition, set the search path to
        uber, public.

        Return True if the connection was made successfully, False otherwise.
        I.e., do NOT throw an error if making the connection fails.

        >>> a2 = Assignment2()
        >>> # This example will work for you if you change the arguments as
        >>> # appropriate for your account.
        >>> a2.connect("csc343h-dianeh", "dianeh", "")
        True
        >>> # In this example, the connection cannot be made.
        >>> a2.connect("nonsense", "silly", "junk")
        False
        """
        try:
            self.connection = pg.connect(
                dbname=dbname, user=username, password=password,
                options="-c search_path=uber,public"
            )
            # This allows psycopg2 to learn about our custom type geo_loc.
            self._register_geo_loc()
            return True
        except pg.Error:
            return False

    def disconnect(self) -> bool:
        """Close the database connection.

        Return True if closing the connection was successful, False otherwise.
        I.e., do NOT throw an error if closing the connection failed.

        >>> a2 = Assignment2()
        >>> # This example will work for you if you change the arguments as
        >>> # appropriate for your account.
        >>> a2.connect("csc343h-dianeh", "dianeh", "")
        True
        >>> a2.disconnect()
        True
        >>> a2.disconnect()
        False
        """
        try:
            if not self.connection.closed:
                self.connection.close()
            return True
        except pg.Error:
            return False

    # ======================= Driver-related methods ======================= #

    def clock_in(self, driver_id: int, when: datetime, geo_loc: GeoLoc) -> bool:
        """Record the fact that the driver with id <driver_id> has declared that
        they are available to start their shift at date time <when> and with
        starting location <geo_loc>. Do so by inserting a row in both the
        ClockedIn and the Location tables.

        If there are no rows are in the ClockedIn table, the id of the shift
        is 1. Otherwise, it is the maximum current shift id + 1.

        A driver can NOT start a new shift if they have an ongoing shift.

        Return True if clocking in was successful, False otherwise. I.e., do NOT
        throw an error if clocking in fails.

        Precondition:
            - <when> is after all dates currently recorded in the database.
        """



        #written code below
        cursor = self.connection.cursor()
    
        ##########################################################
        
        try:
            #check the input parameters are valid
            
            #if the driver not exist in the driver table
            #try:
            cursor.execute("SELECT driver_id FROM Driver where driver_id = %s;",[driver_id])
            check_all_driver_id = cursor.fetchall()
            if check_all_driver_id == []: # empty table means driver not exist in driver table then return false and do nothing
                self.connection.commit()
                return False
            """
            except pg.Error as ex:
                self.connection.rollback()
                return False
            finally:
                cursor.close()
            """

            




            # TODO: implement this method
            #to check if the is empty
            cursor.execute("SELECT shift_id, driver_id FROM ClockedIn ORDER BY shift_id;")
            cInIfEmptyShiftId = cursor.fetchall()
            if cInIfEmptyShiftId == []: # empty table, then check in
                shift_id = 1 
                cursor.execute("INSERT INTO ClockedIn VALUES (%s, %s, %s);", [shift_id, driver_id, when])
                #print("here")
                cursor.execute("INSERT INTO Location VALUES (%s, %s, %s);", [shift_id, when, geo_loc])
                #
                self.connection.commit()
                #cursor.close()
                return True
            
            
            #print("00000")
            #find the higest shift_id for this driver
            #print(driver_id)
            cursor.execute("SELECT shift_id, driver_id FROM ClockedIn where driver_id = %s ORDER BY shift_id DESC LIMIT 1;", [driver_id])
            #cursor.execute("SELECT shift_id, driver_id FROM ClockedIn ORDER BY shift_id DESC LIMIT 1;")
            #print("0 to a")
            cInShiftId = cursor.fetchall()

            #print("aaaa")
            if cInShiftId == []: #means this driver not exist in the checkedIn table yet, then check in
                cursor.execute("SELECT shift_id, driver_id FROM ClockedIn ORDER BY shift_id DESC LIMIT 1;")
                lasgestShiftId = cursor.fetchall() 
                #print( lasgestShiftId )
                #print(lasgestShiftId[0][0] )
                shift_id = lasgestShiftId[0][0] + 1  
                cursor.execute("INSERT INTO ClockedIn VALUES (%s, %s, %s);", [shift_id, driver_id, when])
                cursor.execute("INSERT INTO Location VALUES (%s, %s, %s);", [shift_id, when, geo_loc])
                self.connection.commit()
                #cursor.close()
                return True
            
            #print("abcd ")

            #means cInShiftId != None
            #print(cInShiftId)
            shift_id = cInShiftId[0][0]
            #if gets here, then it means not empty table, shift_id exist in checkin, now check if in the clockout table
            cursor.execute("SELECT shift_id FROM ClockedOut where shift_id = %s ORDER BY shift_id DESC LIMIT 1;", [shift_id])
            shiftIdInCOut = cursor.fetchall()
            #print("hereC")
            if shiftIdInCOut == []: #means the same shift_id exist in checked in but not checkout , so return false as handout requires
                #print("Error:Clocking Failed")
                self.connection.commit()
                #cursor.close()
                return False
            elif shiftIdInCOut != []:
                cursor.execute("SELECT shift_id, driver_id FROM ClockedIn ORDER BY shift_id DESC LIMIT 1;")
                lasgestShiftId = cursor.fetchall() 
                shift_id = lasgestShiftId[0][0] + 1
                cursor.execute("INSERT INTO ClockedIn VALUES (%s, %s, %s);", [shift_id, driver_id, when])
                cursor.execute("INSERT INTO Location VALUES (%s, %s, %s);", [shift_id, when, geo_loc])
                self.connection.commit()
                #cursor.close()
                return True
            
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            print("the error captured by the except")
            self.connection.rollback()
            return False
        finally:
                cursor.close()


        #written code above



        try:
            # TODO: implement this method
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return False

    def pick_up(self, driver_id: int, client_id: int, when: datetime) -> bool:
        """Record the fact that the driver with driver id <driver_id> has
        picked up the client with client id <client_id> at date time <when>.

        If (a) the driver is currently on an ongoing shift, and
           (b) they have been dispatched to pick up the client, and
           (c) the corresponding pick-up has not been recorded
        record it by adding a row to the Pickup table, and return True.
        Otherwise, return False.

        You may not assume that the dispatch actually occurred, but you may
        assume there is no more than one outstanding dispatch entry for this
        driver and this client.

        Return True if the operation was successful, False otherwise. I.e.,
        do NOT throw an error if this pick up fails.

        Precondition:
            - <when> is after all dates currently recorded in the database.
        """

         #written code below
        cursor = self.connection.cursor()

        try:
            # TODO: implement this method
            #check the input parameters
            #if the driver not exist in the driver table
            cursor.execute("SELECT driver_id FROM Driver where driver_id = %s;",[driver_id] )
            check_all_driver_id = cursor.fetchall()
            if check_all_driver_id == []: # empty table means driver not exist in driver table then return false and do nothing
                self.connection.commit()
                cursor.close()
                return False

            #if the client not exist in the driver table
            cursor.execute("SELECT client_id FROM Client where client_id = %s;", [client_id])
            check_all_client_id = cursor.fetchall()
            if check_all_client_id == []: # empty table means driver not exist in driver table then return false and do nothing
                self.connection.commit()
                cursor.close()
                return False
            
            #print("here PICKUP")
            ############# input are checked to be correct now #########################################

            #check if the driver id is in clockedIn and not in clockedout
            cursor.execute("SELECT shift_id, driver_id FROM ClockedIn where driver_id = %s ORDER BY shift_id DESC LIMIT 1;", [driver_id])
            cInShiftId = cursor.fetchall()
            if cInShiftId == []: # shift_id does not exist in the clockedin
                # then it means is is not on onging shift
                self.connection.commit()
                cursor.close()
                return False
            #print("here at 0 before a")

            #if code went through here, it means cInShiftId != None, which means max shift_id for this driver 
            #exist in the clockedIn table. the next thing to do is to check if find the match in clockout table
            #print(cInShiftId)
            shift_id = cInShiftId[0][0]
            #print(shift_id)
            #if gets here, then it means not empty table, shift_id exist in checkin, now check if in the clockout table
            cursor.execute("SELECT shift_id FROM ClockedOut where shift_id = %s ORDER BY shift_id DESC LIMIT 1;", [shift_id])
            shiftIdInCOut = cursor.fetchall()
            #print(shiftIdInCOut)
            #print("here at a")
            
            # match to both exist in clockedIn andclockedOut
            if shiftIdInCOut != []: 
                self.connection.commit()
                cursor.close()
                return False                 
                #print("Error:Clocking Failed")             
                
            # Not found !!! could not find the match both exist in checked in but not checkout , so return false as handout requires   
            elif shiftIdInCOut == []: 
                #print("here at b")
                # check the b) if the client and driver both exist in the dispatch table 
                cursor.execute("SELECT Dispatch.request_id, Request.client_id, ClockedIn.driver_id FROM Dispatch, Request, ClockedIn WHERE Dispatch.request_id = Request.request_id AND ClockedIn.shift_id = Dispatch.shift_id AND Request.client_id = %s AND ClockedIn.driver_id = %s ORDER BY Dispatch.request_id DESC LIMIT 1;",[client_id, driver_id])
                driver_client_dispatch = cursor.fetchall()
                #print(client_id)
                #print(driver_id)
                #print(driver_client_dispatch)
                

                if driver_client_dispatch == []: 
                    self.connection.commit()
                    cursor.close()
                    return False    
                
                elif driver_client_dispatch != []: #the (b) condition passed, now we can move on to check the (c) condition
                    request_id = driver_client_dispatch[0][0]
                    #print("below is driver_client_dispatch")
                    #print(driver_client_dispatch)
                    #print("below is request id: ")
                    #print(request_id)
                    #print("here at c")
                    cursor.execute("SELECT request_id FROM Pickup where request_id = %s ORDER BY request_id DESC LIMIT 1;", [request_id])
                    shiftIdInCOut = cursor.fetchall()
                    #print("here at c to d]")
                    #print(shiftIdInCOut)
                    #print(driver_id)
                    #print(client_id)
                    if shiftIdInCOut != []: #return fail becase this request is alredy recorded
                        self.connection.commit()
                        cursor.close()
                        return False

                    elif shiftIdInCOut == []:
                        #print("here at d]")
                        #print(request_id)
                        cursor.execute("INSERT INTO Pickup VALUES (%s, %s);", [request_id, when])
                        self.connection.commit()
                        cursor.close()
                        return True
                        

            
            #below are some notes for review: 
            #the querry to check if both client and driver under the same dispatch
            #b =("SELECT Dispatch.request_id, Request.client_id, ClockedIn.driver_id FROM Dispatch, Request, ClockedIn WHERE Dispatch.request_id = Request.request_id AND ClockedIn.shift_id = Dispatch.shift_id AND Request.client_id = %s AND ClockedIn.driver_id = %s",[client_id, driver_id])
            
                """
            ("
            SELECT Dispatch.request_id, Request.client_id, ClockedIn.driver_id
            FROM Dispatch, Request, ClockedIn
            WHERE Dispatch.request_id = Request.request_id AND ClockedIn.shift_id = Dispatch.shift_id
                AND Request.client_id = %s AND ClockedIn.driver_id = %s
            "[client_id, driver_id]  )
            """
            pass


        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return False

    # ===================== Dispatcher-related methods ===================== #

    def dispatch(self, nw: GeoLoc, se: GeoLoc, when: datetime) -> None:
        """Dispatch drivers to the clients who have requested rides in the area
        bounded by <nw> and <se>, such that:
            - <nw> is the longitude and latitude in the northwest corner of this
            area
            - <se> is the longitude and latitude in the southeast corner of this
            area
        and record the dispatch time as <when>.

        Area boundaries are inclusive. For example, the point (4.0, 10.0)
        is considered within the area defined by
                    NW = (1.0, 10.0) and SE = (25.0, 2.0)
        even though it is right at the upper boundary of the area.

        NOTE: + longitude values decrease as we move further west, and
                latitude values decrease as we move further south.
              + You may find the PostgreSQL operators @> and <@> helpful.

        For all clients who have requested rides in this area (i.e., whose
        request has a source location in this area) and a driver has not
        been dispatched to them yet, dispatch drivers to them one at a time,
        from the client with the highest total billings down to the client
        with the lowest total billings, or until there are no more drivers
        available.

        Only drivers who meet all of these conditions are dispatched:
            (a) They are currently on an ongoing shift.
            (b) They are available and are NOT currently dispatched or on
            an ongoing ride.
            (c) Their most recent recorded location is in the area bounded by
            <nw> and <se>.
        When choosing a driver for a particular client, if there are several
        drivers to choose from, choose the one closest to the client's source
        location. In the case of a tie, any one of the tied drivers may be
        dispatched.

        Dispatching a driver is accomplished by adding a row to the Dispatch
        table. The dispatch car location is the driver's most recent recorded
        location. All dispatching that results from a call to this method is
        recorded to have happened at the same time, which is passed through
        parameter <when>.

        If an exception occurs during dispatch, rollback ALL changes.

        Precondition:
            - <when> is after all dates currently recorded in the database.
        """

        #written code below#####################################
        cursor = self.connection.cursor()
        try:
            # TODO: Implement this method
            cursor.execute("SELECT request_id, shift_id FROM Dispatch ORDER BY request_id DESC LIMIT 1;")
            lasgestRequest_id = cursor.fetchall() 
            if lasgestRequest_id != []:
                request_id = lasgestRequest_id[0][0] + 1
                shift_id = request_id
                cursor.execute("INSERT INTO Dispatch VALUES (%s, %s, %s, %s);", [request_id, shift_id, '(-20.5, 45.0)', when])
                self.connection.commit()
                return 
            #self.connection.commit()
            return 

        ##########################################################


         #written code above #####################################    
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            
            self.connection.rollback()
            return 
        finally:
            cursor.close()

    # =======================     Helper methods     ======================= #

    # You do not need to understand this code. See the doctest example in
    # class GeoLoc (look for ">>>") for how to use class GeoLoc.

    def _register_geo_loc(self) -> None:
        """Register the GeoLoc type and create the GeoLoc type adapter.

        This method
            (1) informs psycopg2 that the Python class GeoLoc corresponds
                to geo_loc in PostgreSQL.
            (2) defines the logic for quoting GeoLoc objects so that you
                can use GeoLoc objects in calls to execute.
            (3) defines the logic of reading GeoLoc objects from PostgreSQL.

        DO NOT make any modifications to this method.
        """

        def adapt_geo_loc(loc: GeoLoc) -> pg_ext.AsIs:
            """Convert the given geographical location <loc> to a quoted
            SQL string.
            """
            longitude = pg_ext.adapt(loc.longitude)
            latitude = pg_ext.adapt(loc.latitude)
            return pg_ext.AsIs(f"'({longitude}, {latitude})'::geo_loc")

        def cast_geo_loc(value: Optional[str], *args: List[Any]) \
                -> Optional[GeoLoc]:
            """Convert the given value <value> to a GeoLoc object.

            Throw an InterfaceError if the given value can't be converted to
            a GeoLoc object.
            """
            if value is None:
                return None
            m = re.match(r"\(([^)]+),([^)]+)\)", value)

            if m:
                return GeoLoc(float(m.group(1)), float(m.group(2)))
            else:
                raise pg.InterfaceError(f"bad geo_loc representation: {value}")

        with self.connection, self.connection.cursor() as cursor:
            cursor.execute("SELECT NULL::geo_loc")
            geo_loc_oid = cursor.description[0][1]

            geo_loc_type = pg_ext.new_type(
                (geo_loc_oid,), "GeoLoc", cast_geo_loc
            )
            pg_ext.register_type(geo_loc_type)
            pg_ext.register_adapter(GeoLoc, adapt_geo_loc)


def sample_test_function() -> None:
    """A sample test function."""
    a2 = Assignment2()
    try:
        # TODO: Change this to connect to your own database:
        connected = a2.connect("csc343h-xuchao9", "xuchao9", "")
        print(f"[Connected] Expected True | Got {connected}.")

        # TODO: Test one or more methods here, or better yet, make more testing
        #   functions, with each testing a different aspect of the code.

        # ------------------- Testing Clocked In -----------------------------#

        # These tests assume that you have already loaded the sample data we
        # provided into your database.

        # This driver doesn't exist in db
        clocked_in = a2.clock_in(
            989898, datetime.now(), GeoLoc(-79.233, 43.712)
        )
        print(f"[ClockIn] Expected False | Got {clocked_in}.")

        # This drive does exist in the db
        clocked_in = a2.clock_in(
            22222, datetime.now(), GeoLoc(-79.233, 43.712)
        )
        print(f"[ClockIn] Expected True | Got {clocked_in}.")

        # Same driver clocks in again
        clocked_in = a2.clock_in(
            22222, datetime.now(), GeoLoc(-79.233, 43.712)
        )
        print(f"[ClockIn] Expected False | Got {clocked_in}.")

    finally:
        a2.disconnect()



def sample_test_functionClockedIN() -> None:
    """A sample test function."""
    a2 = Assignment2()
    try:
        # TODO: Change this to connect to your own database:
        connected = a2.connect("csc343h-xuchao9", "xuchao9", "")
        print(f"[Connected] Expected True | Got {connected}.")

        # TODO: Test one or more methods here, or better yet, make more testing
        #   functions, with each testing a different aspect of the code.

        # ------------------- Testing Clocked In -----------------------------#

        # These tests assume that you have already loaded the sample data we
        # provided into your database.
        '''
        # This driver doesn't exist in db
        clocked_in = a2.clock_in(
            989898, datetime.now(), GeoLoc(-79.233, 43.712)
        )
        print(f"[ClockIn] Expected False | Got {clocked_in}.")

        # This drive does exist in the db
        clocked_in = a2.clock_in(
            22222, datetime.now(), GeoLoc(-79.233, 43.712)
        )
        print(f"[ClockIn] Expected True | Got {clocked_in}.")

        # Same driver clocks in again
        clocked_in = a2.clock_in(
            22222, datetime.now(), GeoLoc(-79.233, 43.712)
        )
        print(f"[ClockIn] Expected False | Got {clocked_in}.")
        '''
        '''
        #written code below
        cursor = self.connection.cursor()
        cursor.execute("psql csc343h-xuchao9;")
        cursor.execute("\i schema.ddl;")
        cursor.execute("\i data.sql;")
        # Commit the change to the database.
        self.connection.commit()
        # Close the cursor and our connection to the database.
        cursor.close()
        '''
        """
        clocked_in1 = a2.clock_in(
            1111, '2023-07-01 07:55', GeoLoc(-79.1, 43.1)
        )
        
        clocked_in2 = a2.clock_in(
            2222, '2023-07-02 07:55', GeoLoc(-79.2, 43.2)
        )
        """
        #pick_up1 = a2.pick_up(2222, 99, '2019-07-01 07:55')
        #pick_up2 = a2.pick_up(9090, 8877 , '2024-07-03 13:30')
        pick_up2 = a2.pick_up(12345, 100 , '2022-10-31 08:15:00')
        print("worked")

    finally:
        a2.disconnect()



#if __name__ == "__main__":
    # Un comment-out the next two lines if you would like all the doctest
    # examples (see ">>>" in the method and class docstrings) to be run
    # and checked.
    # import doctest
    # doctest.testmod()

    # TODO: Put your testing code here, or call testing functions such as
    #   this one:
    #sample_test_function()
    #sample_test_functionClockedIN()
    #print("Hi")
    

