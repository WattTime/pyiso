from pyiso import client_factory
from unittest import TestCase
from io import StringIO
from datetime import datetime, date
from urllib2 import HTTPError
import logging
import pytz


one_day = StringIO(u"""
<table><tr><td><p>08/02/2015</p></td></tr><tr><td><html>
    <head>
                <style>
                    .dataTable th{
                        color: White; font-weight: bold; background-color: #0055aa;
                        font-size: 12px;
                    }
                    .dataTable td {
                        font-size: 12px;
                    }
                    .dataTable tr.newSection td{

                        color: black; font-weight: bold; background-color: #ffeeaa !important;
                    }
                </style>
    </head>
    <body>
                    <div class="dataDiv">
                        <table ID="dataTable" class="dataTable">
                                        <tr class="newsection">
                                            <td colspan="27" class="sectioninfo1">
                                                2015-08-02
                                            </td>
                                        </tr>
                                        <tr class="header"><th align="center" valign="bottom">Determinant Name</th><th align="center" valign="bottom">Counterparty</th><th align="center" valign="bottom">01</th><th align="center" valign="bottom">02</th><th align="center" valign="bottom">03</th><th align="center" valign="bottom">04</th><th align="center" valign="bottom">05</th><th align="center" valign="bottom">06</th><th align="center" valign="bottom">07</th><th align="center" valign="bottom">08</th><th align="center" valign="bottom">09</th><th align="center" valign="bottom">10</th><th align="center" valign="bottom">11</th><th align="center" valign="bottom">12</th><th align="center" valign="bottom">13</th><th align="center" valign="bottom">14</th><th align="center" valign="bottom">15</th><th align="center" valign="bottom">16</th><th align="center" valign="bottom">17</th><th align="center" valign="bottom">18</th><th align="center" valign="bottom">19</th><th align="center" valign="bottom">20</th><th align="center" valign="bottom">21</th><th align="center" valign="bottom">22</th><th align="center" valign="bottom">23</th><th align="center" valign="bottom">24</th><th align="center" valign="bottom">Total</th></tr>
                                            <tr class="even         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Actual Native Load
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3737
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3534
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3376
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3249
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3183
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3183
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3065
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3225
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3591
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3972
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4360
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4752
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5079
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5121
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5607
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5892
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6020
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6034
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            76980
                                                </td>
                                    </tr>
                                            <tr class="odd         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Actual System Load
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4329
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4114
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3954
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3828
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3761
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3750
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3608
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3770
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4152
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4543
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4934
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5323
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5646
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5702
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6195
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6484
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6617
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6629
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            87339
                                                </td>
                                    </tr>
                                            <tr class="even         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Forecast Native Load
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4202
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3930
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3777
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3622
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3527
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3466
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3340
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3481
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3787
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4190
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4508
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4867
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5171
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5458
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5677
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5888
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5967
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6064
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5728
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5456
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5328
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5059
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4708
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4219
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            111420
                                                </td>
                                    </tr>
                                            <tr class="odd         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Forecast System Load
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4752
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4480
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4327
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4172
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4077
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4016
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    3890
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4031
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4337
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4740
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5058
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5417
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5721
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6008
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6227
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6438
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6517
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6614
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6278
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    6006
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5878
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5609
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    5258
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    4769
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            124620
                                                </td>
                                    </tr>
                                            <tr class="even         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Tie Line
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                        BPA
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -133
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -129
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -124
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -125
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -135
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -144
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -141
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -134
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -123
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -133
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -147
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -152
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -162
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -174
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -156
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -167
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -169
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -167
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            -2615
                                                </td>
                                    </tr>
                                            <tr class="odd         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Tie Line
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                        CAISO
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -137
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -118
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -100
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -113
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -95
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -96
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -72
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -128
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -199
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -310
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -315
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -327
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -379
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -346
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -315
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -289
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -290
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -338
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            -3967
                                                </td>
                                    </tr>
                                            <tr class="even         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Tie Line
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                        LADWP
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -342
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -325
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -285
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -266
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -208
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -190
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -86
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -279
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -259
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -197
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -207
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -165
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -165
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -206
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -225
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -256
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -288
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -234
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            -4183
                                                </td>
                                    </tr>
                                            <tr class="odd         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Tie Line
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                        IPCO
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -56
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -62
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -61
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -52
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -70
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -75
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -58
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -33
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -12
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -6
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    17
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    70
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    82
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    89
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    137
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    139
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    135
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    128
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            312
                                                </td>
                                    </tr>
                                            <tr class="even         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Tie Line
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                        PACE
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -213
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -243
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -274
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -248
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -264
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -255
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -203
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -203
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -186
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -169
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -145
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -185
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -170
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -126
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -104
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -127
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -152
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -173
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            -3440
                                                </td>
                                    </tr>
                                            <tr class="odd         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Tie Line
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                        NVE
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -36
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    24
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    17
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -16
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -31
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -50
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -73
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -73
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -101
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -128
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -106
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    -7
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    23
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    63
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    127
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    176
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    240
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    240
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            289
                                                </td>
                                    </tr>
                                            <tr class="even         ">
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                                Tie Line
                                                </td>
                                                <td align="left"   class="RegularFS BlackFC  ">
                                                        WALC
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    396
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    456
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    534
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    547
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    621
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    656
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    756
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    538
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    466
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    438
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    386
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    364
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    336
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    257
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    182
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    194
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    249
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    277
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td nowrap align="right" class="   RegularFS BlackFC   ">
                                                    0
                                                </td>
                                                <td align="right"   class="RegularFS BlackFC  ">
                                                            7653
                                                </td>
                                    </tr>
                        </table>
                    </div>
    </body>
</html>
</td></tr></table>
""")


class TestNVEnergy(TestCase):
    def setUp(self):
        self.c = client_factory('NEVP')
        handler = logging.StreamHandler()
        self.c.logger.addHandler(handler)
        self.c.logger.setLevel(logging.INFO)

        self.this_date = date(2015, 8, 2)

    def test_idx2ts(self):
        # hour 01 is midnight local
        expected_local_01 = pytz.timezone('US/Pacific').localize(datetime(2015, 8, 2, 0))
        self.assertEqual(self.c.idx2ts(self.this_date, '01'),
                         expected_local_01.astimezone(pytz.utc))

        # hour 24 is 11pm local
        expected_local_24 = pytz.timezone('US/Pacific').localize(datetime(2015, 8, 2, 23))
        self.assertEqual(self.c.idx2ts(self.this_date, '24'),
                         expected_local_24.astimezone(pytz.utc))

        # bad hour errors
        self.assertRaises(ValueError, self.c.idx2ts, self.this_date, 'nothour')

    def test_fetch_df_good(self):
        # set up df from StringIO
        one_day.seek(0)
        df = self.c.fetch_df(one_day)

        # test index and columns
        self.assertEqual(list(df.index),
                         [u'Actual Native Load', u'Actual System Load',
                          u'Forecast Native Load', u'Forecast System Load',
                          u'Tie Line', u'Tie Line', u'Tie Line', u'Tie Line',
                          u'Tie Line', u'Tie Line', u'Tie Line'])
        self.assertEqual(list(df.columns),
                         [u'Counterparty', u'01', u'02', u'03', u'04',
                          u'05', u'06', u'07', u'08', u'09',
                          u'10', u'11', u'12', u'13', u'14',
                          u'15', u'16', u'17', u'18', u'19',
                          u'20', u'21', u'22', u'23', u'24',
                          u'Total'])

    def test_fetch_df_bad(self):
        # no data in year 2020
        self.assertRaises(HTTPError, self.c.fetch_df,
                          self.c.BASE_URL + 'native_system_load_and_ties_for_01_01_2020_.html')

    def test_parse_df(self):
        # set up df from StringIO
        one_day.seek(0)
        df = self.c.fetch_df(one_day)

        # set up options
        self.c.handle_options(latest=True)
        data = self.c.parse_df(df, self.this_date)

        # test
        self.assertEqual(len(data), 18)
        for idp, dp in enumerate(data):
            self.assertEqual(dp['market'], 'RTHR')
            self.assertEqual(dp['freq'], '1hr')
            self.assertEqual(dp['ba_name'], 'NVEnergy')
            self.assertEqual(dp['load_MW'], df.ix['Actual System Load', '%02d' % (idp+1)])
