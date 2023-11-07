CREATE OR REPLACE PROCEDURE ComputeAvgHours (
    dept_number IN NUMBER,
    MaxHours OUT NUMBER
)
AS
    CURSOR C1 IS
        SELECT 
            P.Pnumber as proj_number,
            ROUND(AVG(Hours), 2) AS avg_hrs
        FROM
            DEPARTMENT D
            JOIN PROJECT P ON D.Dnumber = P.Dnum
            JOIN WORKS_ON W ON P.Pnumber = W.Pno
        WHERE
            D.Dnumber = dept_number
        GROUP BY
            D.Dnumber, P.Pnumber
        ORDER BY
            P.Pnumber ASC;
BEGIN
    dbms_output.put_line('received dept no: ' || dept_number);
    dbms_output.put_line('dept_number       project_number      average_hours');
    dbms_output.put_line('---------------------------------------------------');
    MaxHours := 0;
    FOR R1 IN C1 LOOP
        EXIT WHEN C1 % NOTFOUND;
        dbms_output.put_line(dept_number || ' ' || R1.proj_number || ' ' || R1.avg_hrs);
        IF R1.avg_hrs > MaxHours THEN
            MaxHours := R1.avg_hrs;
        END IF;
    END LOOP;
END;
/