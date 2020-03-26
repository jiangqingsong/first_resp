package com.boyun.data

object FeatuerCaseClass {

}

case class GroupFeature2(
                          group_phone: String,
                          imei_sum: String,
                          com_imei: String,
                          vcell_sum: String,
                          vcellular_sum: String,
                          vlac_sum: String,
                          vmsc_sum: String,
                          vcity_sum: String,
                          com_vcity: String,
                          high_risk_prov_sum: String
                        )

case class GroupFeature(
                         group_phone: String,
                         imei: String,
                         cell: String,
                         cellularflag: String,
                         lac: String,
                         msc: String,
                         province: String,
                         roaming_cityid: String,
                         roaming_city: String
                       )

case class CommonFeats(
                        imei: String,
                        cell: String,
                        cellularflag: String,
                        lac: String,
                        msc: String,
                        roaming_cityid: String,
                        roaming_city: String
                      )

case class CalledFeats(
                        calledPhone: String,
                        called_sum: String,
                        called_dura_sum: String,
                        called_dura_per_call: String,
                        called_m_sum: String,
                        called_m_rate: String,
                        called_f_sum: String,
                        called_f_rate: String,
                        called_o_sum: String
                      )

case class CallerFeats(
                        callerPhone: String,
                        call_sum: String,
                        call_per_vcell: String,
                        call_per_vcellular: String,
                        call_per_vlac: String,
                        call_per_vmsc: String,
                        call_per_vcity: String,
                        dura_sum: String,
                        dura_avg: String,
                        call_m_sum: String,
                        dura_m_sum: String,
                        dura_m_avg: String,
                        dura_2s: String,
                        dura_2s_rate: String,
                        dura_3to10s: String,
                        dura_3to10s_rate: String,
                        call_m_rate: String,
                        call_f_sum: String,
                        dura_f_sum: String,
                        dura_f_avg: String,
                        call_f_rate: String,
                        call_o_sum: String,
                        dura_o_sum: String,
                        dura_o_avg: String,
                        opp_phone_sum: String,
                        call_per_phone: String,
                        opp_m_phone_sum: String,
                        call_per_m_phone: String,
                        opp_m_phone_rate: String,
                        opp_f_phone_sum: String,
                        call_per_f_phone: String,
                        opp_f_phone_rate: String,
                        opp_o_phone_sum: String,
                        call_per_o_phone: String,
                        opp_o_phone_rate: String,
                        acitve_days: String,
                        acitve_rate: String,
                        call_max: String,
                        call_min: String,
                        call_std: String,
                        called_hcity_sum: String,
                        com_called_hcity: String,
                        com_called_hcity_id: String,
                        worktime_sum: String,
                        worktime_max: String,
                        worktime_min: String,
                        worktime_std: String,
                        call_per_workhour: String
                      )

case class RoamingLog(
                       record_type: String,
                       roaming_cityid: String,
                       caller_cityid: String,
                       called_cityid: String,
                       Intelligentflag: String,
                       link_refrence: String,
                       longcallflag: String,
                       imsi: String,
                       caller_isdn: String,
                       changflag: String,
                       isdn_type: String,
                       pick_solution: String,
                       called_isdn: String,
                       srvtype: String,
                       srvcode: String,
                       Double_srvtype: String,
                       Double_srvcode: String,
                       channel_request: String,
                       channel_use: String,
                       srv_flag: String,
                       activity_code1: String,
                       extendsrv_code1: String,
                       activity_code2: String,
                       extendsrv_code2: String,
                       activity_code3: String,
                       extendsrv_code3: String,
                       activity_code4: String,
                       extendsrv_code4: String,
                       activity_code5: String,
                       extendsrv_code5: String,
                       msc: String,
                       lac: String,
                       cellularflag: String,
                       mobiletypeflag: String,
                       call_date: String,
                       call_starttime: String,
                       call_datetime: String,
                       chargeunit: String,
                       call_data: String,
                       longcall_code: String,
                       other_code: String,
                       roming_cost: String,
                       longcall_cost: String,
                       other_cost: String,
                       bill_item: String,
                       systypeflag: String,
                       speedflag: String,
                       billingflag: String,
                       imei: String,
                       reserve: String,
                       roaming_city: String,
                       caller_city: String,
                       called_city: String,
                       caller_province: String,
                       called_province: String,
                       cell: String,
                       day: String,
                       minute: String
                     )