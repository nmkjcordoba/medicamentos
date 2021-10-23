var http = require('http');
var express = require('express');
var app = express();
var axios = require('axios');
var cont = 0;
var TYPES = require('tedious').TYPES;
const { Connection, Request } = require("tedious");

const config = {
  authentication: {
    options: {
      userName: "desarrollonk@bx8awnh5ce", // update me
      password: "(n3tm3d1k)" // update me
    },
    type: "default"
  },
  server: "bx8awnh5ce.database.windows.net", // update me
  options: {
    database: "netmedik2", //update me
    encrypt: true
  }
};

function consultarAPI(obj,cont){

	if(cont < obj.length){
		var config = {
	  		method: 'get',
	  		url: 'https://api.hubapi.com/contacts/v1/contact/email/'+obj[cont]["Email"]+'/profile?hapikey=bdb0e588-3b37-401a-a970-307139512d88'
      
	    };
	    axios(config)
	        .then(function (response) {
	          //console.log(JSON.stringify(response.data));

	          //console.log(json.length);
	            console.log(response.data.properties.registro_medico.value);
				InsertarRegistroMedico(response.data.properties.registro_medico.value,obj[cont]["user_id"],obj);
	            


	          //res.send(JSON.stringify(response.data));
	        })
	        .catch(function (error) {
	          //console.log(error.response.status);
	          cont++;
	          setTimeout(function(){ consultarAPI(obj,cont); }, 3000);

	        });
	}else{
		console.log("terminó procedimiento");

	}

    
}


function cosultarCorreoUserProvider() {
    const connection = new Connection(config);
    var allRows = "[";
    var resp = "[{";
    connection.on("connect", err => {
      if (err) {
            console.error(err.message);
          } else {
            queryDatabase();
          }
    });

        function queryDatabase() {
          console.log("Reading rows from the Table...");

          // Read all rows from table
          const request = new Request(
          	`select distinct rp.Email,u.user_id from vw_reportes_proveedores rp join 
provider p on rp.provider_id = p.provider_id join
users u on p.person_id=u.person_id where p.retired=0 and rp.Email is not null`,
           /* `select distinct rp.Email,u.user_id from vw_reportes_proveedores rp join 
provider p on rp.provider_id = p.provider_id join
users u on p.person_id=u.person_id where p.retired=0 and rp.Email is not null and
u.user_id not in (51,158,162,185,1246,1283,1296,1302,1342,1416,1442,1511,1679,1683,1684,1744,1875,1947,2116,2220,2237,
2239,2339,
2384,2414,2535,2544,2630,2647,2656,2671,2745,2778,2823,3007,3016,3028,3057,3118,3125,3126,3128,3130,3135,3139,3141,3142,
3143,3145,3154,3168,3181,3185,3187,3188,3194,3198,3212,3213,3222,3227,3234,3236,3241,3245,3246,3250,3265,3268,3269,3275,
3276,3280,3282,3300,3307,3318,3321,3323,3324,3345,3347,3350,3351,3360,3382,3386,3391,3392,3393,3395,3396,3397,3404,
3409,3424,3426,3432,3433,3438,3443,3448,3450,3457,3463,3475,3476,3479,3480,3481,3482,3488,3489,3492,3509,3516,3521,
3522,3525,3529,3535,3536,3544,3548,3552,3557,3563,3571,3581,3588,3598,3603,3609,3610,3611,3615,3640,3643,3644,3647,
3649,3652,3654,3656,3657,3659,3670,3674,3675,3679,3685,3687,3693,3697,3713,3716,3722,3724,3729,3732,3739,3751,3752,
3755,3756,3762,3766,3768,3774,3775,3777,3783,3787,3793,3794,3795,3796,3798,3803,3817,3819,3828,3836,3848,3876,3904,3907,
3908,3913,3914,3920,3921,3929,3933,3935,3950,3960,3968,3970,3984,4003,4006,4007,4011,4024,4029,4042,4044,4058,4060,4068,
4071,4072,4075,4088,4089,4093,4094,4099,4102,4107,4108,4110,4118,4126,4127,4128,4132,4133,4152,4160,4162)`,*/
            (err, rowCount) => {
              if (err) {
                console.error(err.message);
              } else {
                console.log(`${rowCount} row(s) returned`);
              }
            }
          );


          request.on('row', function(columns) {
            var rep = "{";
            columns.forEach(function(column) {
                if(column.metadata.colName != undefined || column.metadata.colName != ''){
                    rep += '"'+column.metadata.colName +'":"'+column.value +'"';
                    if(column.metadata.colName != 'user_id'){
                        rep += ",";
                    }
                }
                 
            });
            rep += '}';
            allRows += rep+','; 
        });

          request.on('doneProc', function (rowCount, more, returnStatus, rows) {
            allRows = allRows.substring(0, allRows.length - 1);
            allRows += ']';
            allRows = allRows.replace(/^\ufeff/g,"");
            objJson = JSON.parse(allRows.trim());
            console.log(objJson);
            connection.close();
     		consultarAPI(objJson,cont);
            //recorrerDataNetmedik(JSON.parse(allRows));
        });


         connection.execSql(request);
        }

  
}

function InsertarRegistroMedico(registro_medico,user_id,obj) {
    console.log("ingresa a insertar registro medico");
    const connection2 = new Connection(config);
    connection2.on("connect", err => {
      if (err) {
            console.error(err.message);
          } else {
          	console.log("entra al else");
            Insertquery(registro_medico,user_id,obj);
          }
        });

        function Insertquery(registro_medico,user_id,obj) {
          console.log("Insertando datos...");

          // Read all rows from table
          const request = new Request(
            "exec sp_ingresar_actualizar_registro_medico @user_id,@registro,@valor",
            (err, rowCount) => {
              if (err) {
                console.error(err.message);
              } else {
                console.log(`${rowCount} row(s) returned`);
                cont++;
                connection2.close();
	            setTimeout(function(){ consultarAPI(obj,cont); }, 2000);
              }

            }
          );
          request.addParameter('user_id', TYPES.Int,user_id);
          request.addParameter('registro', TYPES.VarChar,'Registro Medico');
          request.addParameter('valor', TYPES.VarChar,registro_medico);


         connection2.execSql(request);
        }

  
}

app.listen(3100, function() {
  console.log('puerto 3100!');
  cosultarCorreoUserProvider();
  //cosultarMedicamentosNetmedik();
});