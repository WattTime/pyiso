from pyiso import client_factory
from unittest import TestCase
from io import StringIO
from datetime import datetime, timedelta
try:
    from urllib2 import HTTPError
except ImportError:
    from urllib.error import HTTPError
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

tomorrow = StringIO(u"""
<html><title>NV ENERGY - TOMORROW'S LOAD FORECAST</title>
    <body bgcolor="#ffffff">
    <center><h1>NV ENERGY  -  TOMORROW'S LOAD FORECAST</h1></center>
    <table border="4">
    <tr><th colspan="2">For 08-03-2015 </th><th colspan="4">Hour Ending</th></tr>
    <tr><th bgcolor="#d0d0d0">NAME</th><th bgcolor="#d0d0d0">DATE</th><th bgcolor="#d0d0d0">1</th><th bgcolor="#d0d0d0">2</th><th bgcolor="#d0d0d0">3</th><th bgcolor="#d0d0d0">4</th><th bgcolor="#d0d0d0">5</th><th bgcolor="#d0d0d0">6</th><th bgcolor="#d0d0d0">7</th><th bgcolor="#d0d0d0">8</th><th bgcolor="#d0d0d0">9</th><th bgcolor="#d0d0d0">10</th><th bgcolor="#d0d0d0">11</th><th bgcolor="#d0d0d0">12</th><th bgcolor="#d0d0d0">13</th><th bgcolor="#d0d0d0">14</th><th bgcolor="#d0d0d0">15</th><th bgcolor="#d0d0d0">16</th><th bgcolor="#d0d0d0">17</th><th bgcolor="#d0d0d0">18</th><th bgcolor="#d0d0d0">19</th><th bgcolor="#d0d0d0">20</th><th bgcolor="#d0d0d0">21</th><th bgcolor="#d0d0d0">22</th><th bgcolor="#d0d0d0">23</th><th bgcolor="#d0d0d0">24</th><th bgcolor="#d0d0d0">25</th><th bgcolor="#d0d0d0">TOTAL</th></tr>
    <tr><th bgcolor="#eeeeaa">Forecast Native Load</th><th bgcolor="#eeeeaa">TOTAL</th><td bgcolor="#eeeeaa">4227 </td>
    <td bgcolor="#eeeeaa">3939 </td>
    <td bgcolor="#eeeeaa">3721 </td>
    <td bgcolor="#eeeeaa">3580 </td>
    <td bgcolor="#eeeeaa">3535 </td>
    <td bgcolor="#eeeeaa">3558 </td>
    <td bgcolor="#eeeeaa">3566 </td>
    <td bgcolor="#eeeeaa">3800 </td>
    <td bgcolor="#eeeeaa">4153 </td>
    <td bgcolor="#eeeeaa">4569 </td>
    <td bgcolor="#eeeeaa">4976 </td>
    <td bgcolor="#eeeeaa">5396 </td>
    <td bgcolor="#eeeeaa">5733 </td>
    <td bgcolor="#eeeeaa">6056 </td>
    <td bgcolor="#eeeeaa">6227 </td>
    <td bgcolor="#eeeeaa">6427 </td>
    <td bgcolor="#eeeeaa">6533 </td>
    <td bgcolor="#eeeeaa">6436 </td>
    <td bgcolor="#eeeeaa">6200 </td>
    <td bgcolor="#eeeeaa">5898 </td>
    <td bgcolor="#eeeeaa">5699 </td>
    <td bgcolor="#eeeeaa">5405 </td>
    <td bgcolor="#eeeeaa">5058 </td>
    <td bgcolor="#eeeeaa">4673 </td>
    <td bgcolor="#eeeeaa">0 </td>
    <th bgcolor="#eeeeaa">119365</th></tr>
    <tr><th bgcolor="#eeeeaa">Forecast System Load</th><th bgcolor="#eeeeaa">TOTAL</th><td bgcolor="#eeeeaa">4777 </td>
    <td bgcolor="#eeeeaa">4489 </td>
    <td bgcolor="#eeeeaa">4271 </td>
    <td bgcolor="#eeeeaa">4130 </td>
    <td bgcolor="#eeeeaa">4085 </td>
    <td bgcolor="#eeeeaa">4108 </td>
    <td bgcolor="#eeeeaa">4116 </td>
    <td bgcolor="#eeeeaa">4350 </td>
    <td bgcolor="#eeeeaa">4703 </td>
    <td bgcolor="#eeeeaa">5119 </td>
    <td bgcolor="#eeeeaa">5526 </td>
    <td bgcolor="#eeeeaa">5946 </td>
    <td bgcolor="#eeeeaa">6283 </td>
    <td bgcolor="#eeeeaa">6606 </td>
    <td bgcolor="#eeeeaa">6777 </td>
    <td bgcolor="#eeeeaa">6977 </td>
    <td bgcolor="#eeeeaa">7083 </td>
    <td bgcolor="#eeeeaa">6986 </td>
    <td bgcolor="#eeeeaa">6750 </td>
    <td bgcolor="#eeeeaa">6448 </td>
    <td bgcolor="#eeeeaa">6249 </td>
    <td bgcolor="#eeeeaa">5955 </td>
    <td bgcolor="#eeeeaa">5608 </td>
    <td bgcolor="#eeeeaa">5223 </td>
    <td bgcolor="#eeeeaa">0 </td>
    <th bgcolor="#eeeeaa">132565</th></tr>
    </table>
    <p>Updated 08-03-2015 08:17:00</p>
    NOTE:
    Load Forecasts are generated from forecast tools that use a regression model with the following underlying assumptions:<br />
    historic actual system-wide loads (SYSTEM) and historic actual hourly native load,<br />
    historic actual and forecasted hourly weather data.<br />
    </body>
</html>
""")

one_month = StringIO(u"""
<table><tr><td><html>
    <head>
    </head>
    <body>
                    <div class="dataDiv">
                        <table ID="dataTable" class="dataTable">
                                        <tr class="newsection">
                                            <td colspan="27" class="sectioninfo1">
                                                2015-07-01
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
                                        <tr>
                                            <td colspan="27" style="height: 20px;">
                                            </td>
                                        </tr>
                                        <tr class="newsection">
                                            <td colspan="27" class="sectioninfo1">
                                                2015-07-02
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
                                        <tr>
                                            <td colspan="27" style="height: 20px;">
                                            </td>
                                        </tr>
                                        <tr class="newsection">
                                            <td colspan="27" class="sectioninfo1">
                                                2015-07-31
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

        self.today = datetime(2015, 8, 2, 12, 34)
        self.tomorrow = datetime(2015, 8, 3, 12, 34)
        self.last_month = datetime(2015, 7, 2, 12, 34)
        self.now = pytz.utc.localize(datetime.utcnow())

    def test_idx2ts(self):
        # hour 01 is midnight local
        expected_local_01 = pytz.timezone('US/Pacific').localize(datetime(2015, 8, 2, 0))
        self.assertEqual(self.c.idx2ts(self.today, '01'),
                         expected_local_01.astimezone(pytz.utc))

        # hour 24 is 11pm local
        expected_local_24 = pytz.timezone('US/Pacific').localize(datetime(2015, 8, 2, 23))
        self.assertEqual(self.c.idx2ts(self.today, '24'),
                         expected_local_24.astimezone(pytz.utc))

        # bad hour errors
        self.assertRaises(ValueError, self.c.idx2ts, self.today, 'nothour')

    def test_data_url_future(self):
        # no data after tomorrow
        self.assertRaises(ValueError, self.c.data_url, self.now+timedelta(days=2))

    def test_data_url_tomorrow(self):
        url, mode = self.c.data_url(self.now+timedelta(days=1))
        self.assertEqual(url, self.c.BASE_URL+'tomorrow.htm')
        self.assertEqual(mode, 'tomorrow')

    def test_data_url_today(self):
        url, mode = self.c.data_url(self.now)
        self.assertEqual(url, self.c.BASE_URL+'native_system_load_and_ties_for_%02d_%02d_%04d_.html' % (self.now.month, self.now.day, self.now.year))
        self.assertEqual(mode, 'recent')

    def test_data_url_historical(self):
        last_month_date = self.now-timedelta(days=35)
        url, mode = self.c.data_url(last_month_date)
        self.assertIn(self.c.BASE_URL, url)
        self.assertIn('Monthly_Ties_and_Loads_L_from', url)
        self.assertIn(last_month_date.strftime('%m_01_%Y'), url)
        self.assertEqual(mode, 'historical')

    def test_fetch_df_today(self):
        # set up df from StringIO
        one_day.seek(0)
        df, mode = self.c.fetch_df(self.today, url=one_day, mode='recent')

        # test index and columns
        self.assertEqual(list(df.index),
                         [u'Actual Native Load', u'Actual System Load',
                          u'Forecast Native Load', u'Forecast System Load',
                          u'Tie Line', u'Tie Line', u'Tie Line', u'Tie Line',
                          u'Tie Line', u'Tie Line', u'Tie Line'])
        self.assertEqual(list(df.columns),
                         [u'Counterparty'] + list(range(1, 25)) + [u'Total'])

    def test_fetch_df_bad(self):
        # no data in year 2020
        self.assertRaises(HTTPError, self.c.fetch_df, self.today,
                          self.c.BASE_URL + 'native_system_load_and_ties_for_01_01_2020_.html')

    def test_parse_load_today(self):
        # set up df from StringIO
        one_day.seek(0)
        df, mode = self.c.fetch_df(self.today, url=one_day)

        # set up options
        self.c.handle_options(latest=True)
        data = self.c.parse_load(df, self.today)

        # test
        self.assertEqual(len(data), 18)
        for idp, dp in enumerate(data):
            self.assertEqual(dp['market'], 'RTHR')
            self.assertEqual(dp['freq'], '1hr')
            self.assertEqual(dp['ba_name'], 'NEVP')
            self.assertEqual(dp['load_MW'], df.ix['Actual System Load', idp+1])

    def test_parse_load_tomorrow(self):
        # set up df from StringIO
        tomorrow.seek(0)
        df, mode = self.c.fetch_df(self.tomorrow, tomorrow, 'tomorrow')

        # set up options
        self.c.handle_options(start_at=self.today, end_at=self.tomorrow+timedelta(days=1))
        data = self.c.parse_load(df, self.tomorrow, 'tomorrow')

        # test
        self.assertEqual(len(data), 24)
        for idp, dp in enumerate(data):
            self.assertEqual(dp['market'], 'RTHR')
            self.assertEqual(dp['freq'], '1hr')
            self.assertEqual(dp['ba_name'], 'NEVP')
            self.assertEqual(dp['load_MW'], df.ix['Forecast System Load', idp+1])

    def test_parse_load_last_month(self):
        # set up df from StringIO
        one_month.seek(0)
        df, mode = self.c.fetch_df(self.last_month, one_month, 'historical')

        # set up options
        self.c.handle_options(start_at=self.last_month, end_at=self.last_month+timedelta(days=2))
        data = self.c.parse_load(df, self.last_month)

        # test
        self.assertEqual(len(data), 18)
        for idp, dp in enumerate(data):
            self.assertEqual(dp['market'], 'RTHR')
            self.assertEqual(dp['freq'], '1hr')
            self.assertEqual(dp['ba_name'], 'NEVP')
            self.assertEqual(dp['load_MW'], df.ix['Actual System Load', idp+1])

    def test_parse_trade_today(self):
        # set up df from StringIO
        one_day.seek(0)
        df, mode = self.c.fetch_df(self.today, url=one_day)

        # set up options
        self.c.handle_options(latest=True)
        data = self.c.parse_trade(df, self.today)

        # test
        self.assertEqual(len(data), 18*len(self.c.TRADE_BAS))
        for idp, dp in enumerate(data):
            self.assertEqual(dp['market'], 'RTHR')
            self.assertEqual(dp['freq'], '1hr')
            self.assertIn(dp['dest_ba_name'], self.c.TRADE_BAS.values())

            dest = [k for k, v in self.c.TRADE_BAS.items() if v == dp['dest_ba_name']][0]
            idx = idp % 18 + 1
            self.assertEqual(dp['export_MW'], df.ix[dest, idx])

    def test_parse_trade_tomorrow(self):
        # set up df from StringIO
        tomorrow.seek(0)
        df, mode = self.c.fetch_df(self.tomorrow, tomorrow, 'tomorrow')

        # set up options
        self.c.handle_options(start_at=self.today, end_at=self.tomorrow+timedelta(days=1))

        # no trade data tomorrow
        self.assertRaises(KeyError, self.c.parse_trade, df, self.tomorrow, 'tomorrow')

    def test_parse_trade_last_month(self):
        # set up df from StringIO
        one_month.seek(0)
        df, mode = self.c.fetch_df(self.last_month, one_month, 'historical')

        # set up options
        self.c.handle_options(start_at=self.last_month, end_at=self.last_month+timedelta(days=2))
        data = self.c.parse_trade(df, self.last_month)

        # test
        self.assertEqual(len(data), 18*len(self.c.TRADE_BAS))
        for idp, dp in enumerate(data):
            self.assertEqual(dp['market'], 'RTHR')
            self.assertEqual(dp['freq'], '1hr')
            self.assertIn(dp['dest_ba_name'], self.c.TRADE_BAS.values())

            dest = [k for k, v in self.c.TRADE_BAS.items() if v == dp['dest_ba_name']][0]
            idx = idp % 18 + 1
            self.assertEqual(dp['export_MW'], df.ix[dest, idx])

    def test_time_subset_latest(self):
        """Subset should return all elements with latest ts"""
        self.c.handle_options(latest=True)
        data = [
            {'timestamp': datetime(2015, 8, 13), 'value': 1},
            {'timestamp': datetime(2015, 8, 12), 'value': 2},
            {'timestamp': datetime(2015, 8, 13), 'value': 3},
        ]
        subs = self.c.time_subset(data)
        self.assertEqual(len(subs), 2)
        self.assertIn({'timestamp': datetime(2015, 8, 13), 'value': 1}, subs)
        self.assertIn({'timestamp': datetime(2015, 8, 13), 'value': 3}, subs)

    def test_time_subset_range(self):
        """Subset should return all elements with ts in range, inclusive"""
        self.c.handle_options(start_at=pytz.utc.localize(datetime(2015, 8, 11)),
                              end_at=pytz.utc.localize(datetime(2015, 8, 13)))
        data = [
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 13)), 'value': 1},
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 10)), 'value': 2},
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 15)), 'value': 3},
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 11)), 'value': 4},
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 12)), 'value': 5},
        ]
        subs = self.c.time_subset(data)
        self.assertEqual(len(subs), 3)
        self.assertIn({'timestamp': pytz.utc.localize(datetime(2015, 8, 13)), 'value': 1}, subs)
        self.assertIn({'timestamp': pytz.utc.localize(datetime(2015, 8, 11)), 'value': 4}, subs)
        self.assertIn({'timestamp': pytz.utc.localize(datetime(2015, 8, 12)), 'value': 5}, subs)
