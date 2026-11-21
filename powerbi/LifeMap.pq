// Consulta llamada LifeMap. Import, API local, sin credenciales ni archivos de datos.
let
    BaseUrl = Text.TrimEnd(LifeMapApiUrl,"/"),
    Area = [lat="39.9526", lon="-75.1652", radius_m="50000", from="2019-01-01", to="2022-01-20"],
    Category = "American (New)",
    Get = (Path as text, Query as record) as record => Json.Document(Web.Contents(BaseUrl,
        [RelativePath=Path, Query=Query, IsRetry=true, Timeout=#duration(0,0,0,30), Headers=[Accept="application/json"]])),
    Pages = (Path as text, Query as record) as list => List.Generate(
        () => [Offset=0, Page=Get(Path, Record.Combine({Query,[limit="500",offset="0"]}))],
        each [Page] <> null,
        each if [Page][has_more] then [Offset=[Offset]+500,
            Page=Get(Path,Record.Combine({Query,[limit="500",offset=Text.From([Offset]+500)]}))]
            else [Offset=0,Page=null], each [Page]),
    TableOf = (Rows as list, Columns as list, Types as list) as table =>
        Table.TransformColumnTypes(Table.FromRecords(Rows,Columns,MissingField.UseNull),Types,"en-US"),
    HotPages = List.Buffer(Pages("live/hotspots",Area)),
    HotRows = List.Combine(List.Transform(HotPages,each [hotspots])),
    Windows = List.Combine(List.Transform(HotPages,each [windows])),
    Hotspots = TableOf(HotRows,{"geohash","latitude","longitude","start","end","reviews","negative_percentage","change_percentage_points"},
        {{"geohash",type text},{"latitude",type number},{"longitude",type number},{"start",type datetimezone},{"end",type datetimezone},
         {"reviews",Int64.Type},{"negative_percentage",type number},{"change_percentage_points",type number}}),
    AlertPages = Pages("alerts",Record.Combine({Area,[active="true",min_score="0"]})),
    AlertRows = List.Combine(List.Transform(AlertPages,each [results])),
    Alerts = TableOf(AlertRows,{"business_id","review_id","latitude","longitude","risk_score","event_date","processed_at","model_id","active"},
        {{"business_id",type text},{"review_id",type text},{"latitude",type number},{"longitude",type number},{"risk_score",type number},
         {"event_date",type datetimezone},{"processed_at",type datetimezone},{"model_id",type text},{"active",type logical}}),
    S = Get("survival/" & Uri.EscapeDataString(Category),[method="observed_origin"]),
    Groups = {[Group=Category,Curve=S[primary]]} & List.Transform(S[zones],each [Group=[zone_id],Curve=[curve]]),
    KMRows = List.Combine(List.Transform(Groups,(g)=>List.Transform(g[Curve][points],(p)=>Record.Combine({p,
        [category=Category,group=g[Group],businesses=g[Curve][businesses],total_events=g[Curve][events],
         total_censored=g[Curve][censored],median_days=g[Curve][median_days]]})))),
    KaplanMeier = TableOf(KMRows,{"category","group","time_days","survival","at_risk","events","censored","entered","supported","businesses","total_events","total_censored","median_days"},
        {{"category",type text},{"group",type text},{"time_days",type number},{"survival",type number},{"at_risk",Int64.Type},
         {"events",Int64.Type},{"censored",Int64.Type},{"entered",Int64.Type},{"supported",type logical},{"businesses",Int64.Type},
         {"total_events",Int64.Type},{"total_censored",Int64.Type},{"median_days",type number}}),
    WindowTable = TableOf(Windows,{"start","end","reviews","negative"},{{"start",type datetimezone},{"end",type datetimezone},{"reviews",Int64.Type},{"negative",Int64.Type}}),
    Summed = Table.Group(WindowTable,{"start","end"},{{"reviews",each List.Sum([reviews]),Int64.Type},{"negative",each List.Sum([negative]),Int64.Type}}),
    Evolution = Table.AddColumn(Summed,"negative_percentage",each if [reviews]>0 then 100*[negative]/[reviews] else null,type number),
    Refresh = #table(type table [api_generated_at=datetimezone,loaded_at_utc=datetimezone,hotspots=number,active_alerts=number,windows=number],
        {{DateTimeZone.FromText(HotPages{0}[generated_at]),DateTimeZone.FixedUtcNow(),List.Count(HotRows),List.Count(AlertRows),List.Count(Windows)}})
in
    [Hotspots=Hotspots,Alerts=Alerts,KaplanMeier=KaplanMeier,Evolution=Evolution,Refresh=Refresh]
