CREATE   PROCEDURE Marketing.uspLoadFactIncident
    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)
AS
BEGIN

    DECLARE @run_start DATETIME2(6)=SYSUTCDATETIME();
    DECLARE @rows_read BIGINT=0;
    DECLARE @inserted BIGINT=0;

    BEGIN TRY

        SELECT @rows_read=COUNT(*)
        FROM LH_DRZ_SILVER.fleet.incidents;

        INSERT INTO Marketing.FactIncident
        (
            IncidentID,
            RentalKey,
            VehicleKey,
            CustomerKey,
            IncidentDateKey,
            EstimatedDamageZAR,
            ActualRepairCostZAR,
            InsurancePayoutZAR,
            ExcessChargedZAR,
            Status
        )
        SELECT
            i.incident_id,
            fr.RentalKey,
            dv.VehicleKey,
            dc.CustomerKey,
            YEAR(i.incident_date)*10000+
            MONTH(i.incident_date)*100+
            DAY(i.incident_date),
            i.estimated_damage_zar,
            i.actual_repair_cost_zar,
            i.insurance_payout_zar,
            i.excess_charged_zar,
            i.status
        FROM LH_DRZ_SILVER.fleet.incidents i
        INNER JOIN Marketing.FactRental fr
            ON i.rental_id = fr.RentalID
        INNER JOIN Marketing.DimVehicle dv
            ON i.vehicle_id = dv.VehicleID
           AND dv.IsCurrent = 1
        INNER JOIN Marketing.DimCustomer dc
            ON i.customer_id = dc.CustomerID
           AND dc.IsCurrent = 1
        LEFT JOIN Marketing.FactIncident existing
            ON i.incident_id = existing.IncidentID
        WHERE existing.IncidentID IS NULL;

        SET @inserted = @@ROWCOUNT;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'FactIncident',
            @rows_read,
            @inserted,
            @inserted,
            0,
            @run_start;

    END TRY
    BEGIN CATCH

        DECLARE @ErrorMessage VARCHAR(4000);
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC metadata.usp_LogFailure
            @pipeline_run_id,
            @pipeline_name,
            'FactIncident',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;