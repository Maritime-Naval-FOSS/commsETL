CREATE TABLE comm_imp_vodafone
    (vod_outof_zone                 VARCHAR2(5),
    caller_num                     VARCHAR2(15),
    call_date                      VARCHAR2(15),
    call_time                      VARCHAR2(15),
    call_type                      VARCHAR2(50),
    roaming                        VARCHAR2(20),
    destination                    VARCHAR2(20),
    network                        VARCHAR2(20),
    duration                       VARCHAR2(10),
    data                           VARCHAR2(25),
    call_cost                      VARCHAR2(15),
    id                             NUMBER)
/


-- Triggers for COMM_IMP_VODAFONE

CREATE OR REPLACE TRIGGER comm_imp_vodafone_trg
 BEFORE
  INSERT OR UPDATE
 ON comm_imp_vodafone
REFERENCING NEW AS NEW OLD AS OLD
 FOR EACH ROW
BEGIN
  SELECT decode(nvl(:NEW.ID,0), 0, COMM_IMP_ID.NEXTVAL, :NEW.ID) INTO :NEW.ID from dual;
END;
/


-- Comments for COMM_IMP_VODAFONE

COMMENT ON COLUMN comm_imp_vodafone.id IS 'autonumber from trigger'
/
===============================================================================
