package utils

enum RoadValue {

    LV_D("DAY_LV_HOUR","LV_D"),
    EV_LV_HOUR("EV_LV_HOUR","LV_E"),
    NIGHT_LV_HOUR("NIGHT_LV_HOUR","LV_N"),
    DAY_LV_SPEED("DAY_LV_SPEED","LV_SPD_D"),
    EV_LV_SPEED("EV_LV_SPEED","LV_SPD_E"),
    NIGHT_LV_SPEED("NIGHT_LV_SPEED","LV_SPD_N"),
    DAY_HV_HOUR("DAY_HV_HOUR","HGV_D"),
    EV_HV_HOUR("EV_HV_HOUR","HGV_E"),
    NIGHT_HV_HOUR("NIGHT_HV_HOUR","HGV_N"),
    DAY_HV_SPEED("DAY_HV_SPEED","HGV_SPD_D"),
    EV_HV_SPEED("EV_HV_SPEED","HGV_SPD_E"),
    NIGHT_HV_SPEED("NIGHT_HV_SPEED","HGV_SPD_N"),
    PAVEMENT("PAVEMENT","PVMT"),
    DIRECTION("DIRECTION","WAY");

    private final gcProperties
    private final nmProperties

    private RoadValue(gcProperty,nmProperty){
        this.gcProperties = gcProperty
        this.nmProperties = nmProperty
    }

    public Object getGcProperty(){
        return gcProperties
    }

    public Object getNmProperty(){
        return nmProperties
    }


}