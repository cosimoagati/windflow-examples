(load (compile-file "plotmetrics.lisp"))

(defparameter *sa-directory*
  (pathname "sa-sentiment-analysis/testresults-2022-08-28-17-03/"))
(defparameter *mo-directory*
  (pathname "mo-machine-outlier/testresults-2022-09-23-20-32/data-stream-top-k/"))
(defparameter *tt-directory*
  (pathname "tt-trending-topics/testresults-2022-09-29-23-22/"))
(defparameter *rl-directory*
  (pathname "rl-reinforcement-learner/testresults-2022-09-30-15-52/"))
(defparameter *lp-directory*
  (pathname "lp-log-processing/testresults-2022-09-02-07-01/"))

(defparameter *parallelism-x-label* "Parallelism degree for each node")
(defparameter *throughput-y-label* "Throughput (tuples per second)")
(defparameter *scalability-y-label* "Scalability")

;;; Sentiment Analysis (SA)

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "SA average throughput, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(914581.25 1810770.7 2748242.0 3623771.5 4578529.5 5511013.0 6421764.0 7299102.0 7522324.0 7840571.0 8089628.0 8153471.0 8577864.0 8826029.0 9185283.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(734638.06 1392974.5 2180042.7 2866329.7 3254888.7 4009220.7 4646882.0 5253847.0 5464754.5 5579437.5 5850499.5 6104882.0 6244925.5 6668215.0 6886624.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(990114.6 1997980.2 2970556.2 3909312.2 4890244.0 5934206.5 6777290.0 7746063.0 8197798.5 8507182.0 8975822.0 9314099.0 9673200.0 9995032.0 1.0504489E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1019729.9 2024699.5 3050987.5 3984795.5 5093667.5 6083700.0 7076882.0 8198763.0 8227152.5 8643307.0 9130755.0 9563530.0 9848303.0 1.0319139E+7 1.0710862E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15))))
 (plotmetrics:make-raw-plot-data
  :title "SA average throughput scalability, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *scalability-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(1.0 1.9798905 3.0049186 3.9622195 5.0061483 6.0257225 7.021535 7.980813 8.224883 8.572853 8.845172 8.914977 9.379006 9.650351 10.043157)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(1.0 1.8961371 2.9675057 3.9016895 4.430602 5.4574094 6.325403 7.1516123 7.438703 7.5948114 7.9637847 8.310054 8.500683 9.076871 9.374172)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(1.0 2.0179281 3.0002146 3.948343 4.9390683 5.993454 6.8449545 7.8234005 8.279646 8.592118 9.065436 9.407092 9.769777 10.094823 10.609366)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1.0 1.9855253 2.9919565 3.907697 4.995115 5.9659915 6.939957 8.0401325 8.067972 8.476075 8.954092 9.378493 9.657757 10.119483 10.503627)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";Ideal;with points pt 1 ps 2 lc 'black'"
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)) )))
(vgplot:print-plot (pathname "sa-throughput-batching.png"))

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "SA average throughput"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(914581.25 1810770.7 2748242.0 3623771.5 4578529.5 5511013.0 6421764.0 7299102.0 7522324.0 7840571.0 8089628.0 8153471.0 8577864.0 8826029.0 9185283.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  ;; #(734638.06 1392974.5 2180042.7 2866329.7 3254888.7 4009220.7 4646882.0 5253847.0 5464754.5 5579437.5 5850499.5 6104882.0 6244925.5 6668215.0 6886624.0)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(990114.6 1997980.2 2970556.2 3909312.2 4890244.0 5934206.5 6777290.0 7746063.0 8197798.5 8507182.0 8975822.0 9314099.0 9673200.0 9995032.0 1.0504489E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1019729.9 2024699.5 3050987.5 3984795.5 5093667.5 6083700.0 7076882.0 8198763.0 8227152.5 8643307.0 9130755.0 9563530.0 9848303.0 1.0319139E+7 1.0710862E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";Chaining;with linespoints pt 2 ps 1 lc 'black'"
                  #(812227.75 1657455.0 2475656.0 3274093.2 3982319.5 4824067.5 5574321.5 6472990.5 7333856.5 7903270.5 8928637.0 9593692.0 1.0420992E+7 1.137174E+7 1.2060991E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15))))
 (plotmetrics:make-raw-plot-data
  :title "SA average throughput"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(914581.25 1810770.7 2748242.0 3623771.5 4578529.5 5511013.0 6421764.0 7299102.0 7522324.0 7840571.0 8089628.0 8153471.0 8577864.0 8826029.0 9185283.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  ;; #(734638.06 1392974.5 2180042.7 2866329.7 3254888.7 4009220.7 4646882.0 5253847.0 5464754.5 5579437.5 5850499.5 6104882.0 6244925.5 6668215.0 6886624.0)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(914664.2 1774739.9 2730958.5 3580715.7 4391275.5 5344338.0 6155082.0 6930182.0 7269425.5 7536398.0 7836662.0 8325477.0 8538858.0 8818893.0 9254354.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1019729.9 2024699.5 3050987.5 3984795.5 5093667.5 6083700.0 7076882.0 8198763.0 8227152.5 8643307.0 9130755.0 9563530.0 9848303.0 1.0319139E+7 1.0710862E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                  ";Chaining;with linespoints pt 2 ps 1 lc 'black'"
                  #(812227.75 1657455.0 2475656.0 3274093.2 3982319.5 4824067.5 5574321.5 6472990.5 7333856.5 7903270.5 8928637.0 9593692.0 1.0420992E+7 1.137174E+7 1.2060991E+7
                    1.2949175E+7 1.3513249E+7 1.4153548E+7 1.5690533E+7 1.6045256E+7 1.7171888E+7 1.7913572E+7 1.8655708E+7 1.9654012E+7 1.9173312E+7 2.001112E+7 2.0400764E+7
                    2.060529E+7 2.062342E+7 2.0373536E+7 2.0902118E+7 2.1296702E+7 2.0641584E+7 2.231986E+7 2.2142784E+7 2.2398512E+7 2.2338116E+7 2.2891202E+7 2.3340578E+7 2.3752954E+7
                    2.373512E+7 2.3893702E+7 2.4137334E+7 2.4425316E+7 2.4192916E+7 2.4851588E+7 2.5184158E+7 2.5289236E+7)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48)))))

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "SA average latency, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                           #(35.85916 36.297775 35.799652 36.175533 35.78644 35.677677 35.72037 35.905334 40.47396 43.737625 46.728374 50.27041 51.331676 53.13792 54.12379)
                           #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                           ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                           ;; #(44.61632 47.061497 45.199913 45.757275 50.74786 49.115025 49.440365 50.025368 54.952477 60.29694 63.14271 65.8352 69.812225 70.13883 71.76461)
                           ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                           ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                           #(143.30357 147.66386 143.95566 146.3466 149.20023 147.0331 148.91563 151.1991 165.6718 179.0856 189.49045 194.3658 204.10207 211.33687 213.37994)
                           #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                           ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                           #(528.207 523.59064 527.965 534.7961 533.99646 527.99744 538.958 538.67444 586.2429 632.6912 660.6268 691.1122 716.7674 740.4918 747.95654)
                           #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)
                           ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                           #(2033.627 2047.8594 2035.8365 2075.8784 2025.6359 2032.7783 2033.1315 2003.7892 2288.0757 2434.9375 2532.9949 2625.2708 2738.2947 2799.5664 2854.8237)
                           #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)))))

(plotmetrics:boxplot-subplots
 (plotmetrics:make-parameters :plotdir *sa-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 0
                              :percentiles '(5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *sa-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 4
                              :percentiles '(5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *sa-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 16
                              :percentiles '(5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *sa-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 64
                              :percentiles '(5 25 50 75 95 100)))

;;; Machine Outlier (MO)

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "MO average throughput, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(180229.86 338944.16 465764.25 465200.94 485933.47 405203.2 462605.44 334437.1 461522.06)
                  #(1 2 3 4 5 6 7 8 9)
                  ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  ;; #(175967.83 330741.97 370073.75 488106.03 392464.97 435483.9 459654.8 332159.78 430399.97)
                  ;; #(1 2 3 4 5 6 7 8 9)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(427001.62 737142.9 772175.56 804261.8 668006.94 706789.7 710249.8 449062.06 436251.97)
                  #(1 2 3 4 5 6 7 8 9)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(439443.84 784513.9 767166.9 632302.5 696509.9 656269.5 689809.06 535286.9 471275.1)
                  #(1 2 3 4 5 6 7 8 9)
                  ";DETERMINISTIC;with linespoints pt 2 ps 1 lc 'black'"
                  #(198093.06 470205.25 607011.8 364177.87 455674.94 395720.8 571593.2 415398.3 608360.0)
                  #(1 2 3 4 5 6 7 8 9))))
 (plotmetrics:make-raw-plot-data
  :title "MO average throughput scalability, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *scalability-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(1.0 1.8806216 2.584279 2.5811534 2.6961873 2.2482576 2.5667524 1.8556142 2.5607412)
                  #(1 2 3 4 5 6 7 8 9)
                  ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  ;; #(1.0 1.8795594 2.1030762 2.7738366 2.2303224 2.4747927 2.6121526 1.8876165 2.4459014)
                  ;; #(1 2 3 4 5 6 7 8 9)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(1.0 1.7263234 1.8083668 1.88351 1.5644132 1.6552389 1.6633422 1.0516636 1.0216634)
                  #(1 2 3 4 5 6 7 8 9)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1.0 1.7852426 1.7457677 1.4388698 1.5849804 1.4934092 1.5697318 1.2181008 1.0724353)
                  #(1 2 3 4 5 6 7 8 9)
                  ";DETERMINISTIC;with linespoints pt 2 ps 1 lc 'black'"
                  #(1.0 2.3736582 3.064276 1.8384181 2.3003073 1.9976511 2.885478 2.0969856 3.0710816)
                  #(1 2 3 4 5 6 7 8 9)
                  ";Ideal;with points pt 1 ps 2 lc 'black'"
                  #(1 2 3 4 5 6 7 8 9)
                  #(1 2 3 4 5 6 7 8 9)))))


(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "MO average throughput, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(180229.86 338944.16 465764.25 465200.94 485933.47 405203.2 462605.44 334437.1 461522.06)
                  #(1 2 3 4 5 6 7 8 9)
                  ;; ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  ;; #(175967.83 330741.97 370073.75 488106.03 392464.97 435483.9 459654.8 332159.78 430399.97)
                  ;; #(1 2 3 4 5 6 7 8 9)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(427001.62 737142.9 772175.56 804261.8 668006.94 706789.7 710249.8 449062.06 436251.97)
                  #(1 2 3 4 5 6 7 8 9)
                  ;; ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  ;; #(439443.84 784513.9 767166.9 632302.5 696509.9 656269.5 689809.06 535286.9 471275.1)
                  ;; #(1 2 3 4 5 6 7 8 9)
                  ";DETERMINISTIC;with linespoints pt 2 ps 1 lc 'black'"
                  #(198093.06 470205.25 607011.8 364177.87 455674.94 395720.8 571593.2 415398.3 608360.0)
                  #(1 2 3 4 5 6 7 8 9))))
 (plotmetrics:make-raw-plot-data
  :title "MO average throughput, chaining: enabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(148336.81 341410.94 377874.94 493201.03 443213.87 462188.7 505729.7 436534.2 412625.2 467171.6 422953.22 414832.03 425957.75 297018.56 426737.72 298228.44 408514.16 293030.56 430260.8 290211.7 274012.16 295722.22 274564.5 371615.56)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(126615.89 296583.72 354340.62 633565.6 591836.3 516767.56 539524.5 599510.4 606281.06 663956.25 601133.4 612598.75 543102.8 365111.8 578337.75 392546.34 575769.1 379787.1 562150.8 386132.2 366338.66 390446.25 386992.9 388448.78)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
                  ;; ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  ;; #(112360.62 276162.16 326862.84 432512.3 507905.84 461767.97 565674.2 526030.0 523896.72 588299.56 524653.75 533650.06 472570.9 371646.06 483824.6 396678.47 487300.6 394120.72 519642.37 389150.25 385796.87 415170.84 410220.7 414501.6)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
                  ";DETERMINISTIC;with linespoints pt 2 ps 1 lc 'black'"
                  #(180656.06 325976.72 381616.7 623372.3 501470.0 368429.16 590693.2 539472.7 533633.5 490052.34 556600.6 541112.0 545163.5 379166.4 542774.94 395256.94 544009.9 389156.03 521261.0 376761.06 344111.37 371512.97 357005.4 509715.06)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)))))

(plotmetrics:boxplot-subplots
 (plotmetrics:make-parameters :plotdir *mo-directory*
                              :metric "latency"
                              :chaining-p nil
                              :execmode "deterministic"
                              :percentiles '(5 25 50 75 95))
 (plotmetrics:make-parameters :plotdir *mo-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 0
                              :percentiles '(5 25 50 75 95))
 (plotmetrics:make-parameters :plotdir *mo-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 16
                              :percentiles '(5 25 50 75 95))
 (plotmetrics:make-parameters :plotdir *mo-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 64
                              :percentiles '(5 25 50 75 95)))


(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "MO average latency, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(405.9297 351.2509 309.9822 400.30994 467.16763 725.9683 686.95184 1065.2819 904.93427)
                  #(1 2 3 4 5 6 7 8 9)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(1235.1826 2250.0117 2866.358 3542.11 5133.5156 6302.4146 6709.9844 11876.057 14428.502)
                  #(1 2 3 4 5 6 7 8 9)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(4656.115 7803.6567 11067.473 15797.143 18656.187 24048.053 25200.992 39827.285 49393.895)
                  #(1 2 3 4 5 6 7 8 9)
                  ";DETERMINISTIC;with linespoints pt 2 ps 1 lc 'black'"
                  #(367.9948 230.7219 248.64697 487.9231 489.39352 729.7309 560.6725 866.43146 683.9726)
                  #(1 2 3 4 5 6 7 8 9)))))

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "MO average latency, chaining: enabled"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(220.58888 125.20688 135.10126 108.10094 159.91083 215.58568 194.23888 255.14221 303.3117 297.1214 366.57135 410.01474 431.5875 951.30347 525.18787 974.72003 673.49133 1178.7369 735.4685 1382.683 1779.4784 1530.5367 1795.5781 1386.3867)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(4031.6545 2234.7002 2141.6204 1221.443 1652.1844 2904.0964 2884.4995 2792.607 3220.576 3218.6152 4124.867 4522.407 5353.958 8974.697 5672.642 8973.395 6347.9014 11244.412 7247.4106 12688.795 14955.358 14132.55 15251.691 16163.463)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(17043.469 9652.344 10376.037 8931.151 8870.139 13807.061 10893.473 13070.718 14364.414 14308.244 17793.135 18823.193 25967.715 38387.316 28207.986 36584.652 31535.258 42794.32 32754.338 49311.613 52910.89 48234.004 52084.71 52043.01)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)
                  ";DETERMINISTIC;with linespoints pt 2 ps 1 lc 'black'"
                  #(179.8584 127.9259 126.77137 89.50592 138.64093 267.28613 165.47305 206.32442 237.46933 283.9085 278.35648 308.4974 319.71033 695.60297 385.04517 645.5684 443.74698 785.5919 553.87427 960.1292 1371.8821 1117.4725 1257.455 916.06494)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24) ))))


;; Trending Topics (TT)

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "TT throughput, chaining: no, timers: threads"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1.5 lc 'black'"
                  #(398871.0 844759.1 1267192.1 1724415.0 1640743.4 1690995.5 1904356.9 2252767.0)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 1;with linespoints pt 2 ps 1.5 lc 'black'"
                  #(373408.97 796534.94 1261347.4 1688268.7 1678606.7 1684249.1 1941663.9 2246154.2)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 16;with linespoints pt 4 ps 1.5 lc 'black'"
                  #(427101.22 857638.5 1413028.9 1889948.0 1798716.0 1832842.2 2124132.7 2391740.2)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 64;with linespoints pt 8 ps 2 lc 'black'"
                  #(431884.2 922451.8 1394742.9 1917482.9 1855091.0 1819157.5 2123159.0 2447312.5)
                  #(1 2 3 4 5 6 7 8)) ))
 (plotmetrics:make-raw-plot-data
  :title "TT throughput, chaining: no, timers: nodes"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1.5 lc 'black'"
                  #(432985.2 893839.5 1335070.0 1519883.4 1546145.0 1799738.7 2054146.5)
                  #(1 2 3 4 5 6 7)
                  ";b = 1;with linespoints pt 2 ps 1.5 lc 'black'"
                  #(388950.53 807670.4 1305763.1 1525664.0 1720648.1 1729125.6 2002692.1)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1.5 lc 'black'"
                  #(463278.8 953695.75 1431368.0 1972909.5 1854470.1 1986055.9 2188311.7)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 2 lc 'black'"
                  #(501335.66 921722.9 1404045.5 1870362.7 1882485.6 1804051.7 2267374.7)
                  #(1 2 3 4 5 6 7)) ))
 (plotmetrics:make-raw-plot-data
  :title "TT thr. scalab., chaining: no, timers: threads"
  :x-label *parallelism-x-label*
  :y-label *scalability-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1.5 lc 'black'"
                  #(1.0 2.1178756 3.1769474 4.32324 4.1134686 4.2394547 4.774368 5.647859)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 1;with linespoints pt 2 ps 1.5 lc 'black'"
                  #(1.0 2.1331437 3.3779247 4.5212326 4.4953575 4.510468 5.1998324 6.015266)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 16;with linespoints pt 4 ps 1.5 lc 'black'"
                  #(1.0 2.008045 3.308417 4.425059 4.2114515 4.291353 4.973371 5.599938)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 64;with linespoints pt 8 ps 2 lc 'black'"
                  #(1.0 2.1358778 3.229437 4.439808 4.2953434 4.212142 4.916038 5.666594)
                  #(1 2 3 4 5 6 7 8)
                  ";Ideal;with points pt 1 ps 2 lc 'black'"
                  #(1 2 3 4 5 6 7 8)
                  #(1 2 3 4 5 6 7 8))))
 (plotmetrics:make-raw-plot-data
  :title "TT thr. scalab., chaining: no, timers: nodes"
  :x-label *parallelism-x-label*
  :y-label *scalability-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1.5 lc 'black'"
                  #(1.0 2.0643651 3.0834079 3.5102434 3.5708957 4.156583 4.7441497)
                  #(1 2 3 4 5 6 7)
                  ";b = 1;with linespoints pt 2 ps 1.5 lc 'black'"
                  #(1.0 2.0765374 3.3571444 3.9225142 4.4238224 4.445618 5.1489635)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1.5 lc 'black'"
                  #(1.0 2.0585783 3.089647 4.2585793 4.0029244 4.286956 4.723531)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 2 lc 'black'"
                  #(1.0 1.8385344 2.8006096 3.7307596 3.7549405 3.598491 4.522668)
                  #(1 2 3 4 5 6 7)
                  ";Ideal;with points pt 1 ps 2 lc 'black'"
                  #(1 2 3 4 5 6 7)
                  #(1 2 3 4 5 6 7)))))


(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "TT avg latency, chaining: no, timers: threads"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(2181.8896 2178.0198 2176.497 2176.0376 2202.063 2216.3718 2220.9917 2222.744)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(3468.582 5708.6704 3099.5247 4307.913 3357.4055 4211.7847 3887.376 3681.085)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(3506.9004 3483.9 5998.452 4018.9219 4032.0605 4608.6743 4661.603 4869.12)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(8375.4 8387.828 6945.588 7630.1484 8560.73 10900.656 10066.397 10023.979)
                  #(1 2 3 4 5 6 7 8)) ))
 (plotmetrics:make-raw-plot-data
  :title "TT avg latency, chaining: no, timers: nodes"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(6075.358 6070.09 4074.0 4092.0417 2623.4558 4109.574 4111.3804)
                  #(1 2 3 4 5 6 7)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(6266.105 5231.213 4596.951 4192.918 4723.7134 6405.672 4412.996)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(5114.659 5084.3145 5084.09 7038.9214 7383.624 5353.443 7465.767)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(10058.43 9860.957 8410.763 10327.889 11424.558 12621.691 10083.187)
                  #(1 2 3 4 5 6 7)))))

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "TT avg latency, chaining: yes, timers: threads"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(2088.3872 2099.1946 2102.387 2102.24 2102.5618 2102.269 2102.365 2102.3796 2102.5586 2099.1965 2099.136 2102.956 2103.1313 2103.3345 2103.15 2103.3604)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(2093.1628 2290.7812 2874.276 2555.628 3049.2446 3163.039 2941.4026 2836.717 2320.4546 2649.105 2959.316 3586.707 3866.669 2788.752 3037.0513 3203.6968) #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(3029.2812 2385.752 2610.425 2644.322 3155.7048 2536.3948 3152.359 3292.1882 2180.278 2274.294 2284.8286 3926.5273 2474.6917 3933.783 3173.9233 3109.0234)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ;; ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  ;; #(2212.683 2759.135 2786.462 3146.1978 2650.3699 2561.0066 2287.2195 2944.9424 2197.0798 2974.4258 2547.7854 3126.395 2697.947 3170.9912 3389.0144 3096.093)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  )))
 (plotmetrics:make-raw-plot-data
  :title "TT avg latency, chaining: yes, timers: nodes"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(3999.233 3322.745 2000.7578 5992.828 4000.3015 4001.5747 3431.0054 4002.7747 2005.9282 4005.3013 5952.2695)
                  #(1 2 3 4 5 6 7 8 9 10 11)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(3999.4412 4000.6033 4000.8691 4000.5022 4449.3037 5984.8115 5980.724 5954.745 4732.287 5667.0913 5951.5093)
                  #(1 2 3 4 5 6 7 8 9 10 11)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(5999.252 5601.2505 4000.9087 5993.387 3973.216 3980.1848 4003.1223 5954.2305 4005.563 3769.0562 5953.547)
                  #(1 2 3 4 5 6 7 8 9 10 11)
                  ;; ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  ;; #(6000.073 6000.3794 5329.012 5997.05 4006.6082 5991.3623 3993.7393 5960.367 5976.5015 5968.9287 5884.6313)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11)
                  ))))


(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "TT throughput, chaining: no, timers: threads"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1.5 lc 'black'"
                  #(398871.0 844759.1 1267192.1 1724415.0 1640743.4 1690995.5 1904356.9 2252767.0)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 1;with linespoints pt 2 ps 1.5 lc 'black'"
                  #(373408.97 796534.94 1261347.4 1688268.7 1678606.7 1684249.1 1941663.9 2246154.2)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 16;with linespoints pt 4 ps 1.5 lc 'black'"
                  #(427101.22 857638.5 1413028.9 1889948.0 1798716.0 1832842.2 2124132.7 2391740.2)
                  #(1 2 3 4 5 6 7 8)
                  ";b = 64;with linespoints pt 8 ps 2 lc 'black'"
                  #(431884.2 922451.8 1394742.9 1917482.9 1855091.0 1819157.5 2123159.0 2447312.5)
                  #(1 2 3 4 5 6 7 8)) ))
 (plotmetrics:make-raw-plot-data
  :title "TT throughput, chaining: no, timers: nodes"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1.5 lc 'black'"
                  #(432985.2 893839.5 1335070.0 1519883.4 1546145.0 1799738.7 2054146.5)
                  #(1 2 3 4 5 6 7)
                  ";b = 1;with linespoints pt 2 ps 1.5 lc 'black'"
                  #(388950.53 807670.4 1305763.1 1525664.0 1720648.1 1729125.6 2002692.1)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1.5 lc 'black'"
                  #(463278.8 953695.75 1431368.0 1972909.5 1854470.1 1986055.9 2188311.7)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 2 lc 'black'"
                  #(501335.66 921722.9 1404045.5 1870362.7 1882485.6 1804051.7 2267374.7)
                  #(1 2 3 4 5 6 7)) ))
 (plotmetrics:make-raw-plot-data
  :title "TT throughput, chaining: yes, timers: threads"
  :x-label *parallelism-x-label*
  :y-label *scalability-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1.5 lc 'black'"
                  #(426554.28 754746.9 1158248.0 1568194.6 2031507.6 2480790.0 2901676.7 3200994.7 3338115.2 3175820.0 3228721.5 3241778.0 3509768.7 3651127.5 3986708.7 4298435.0) #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 1;with linespoints pt 2 ps 1.5 lc 'black'"
                  #(428683.34 754054.9 1177195.0 1637235.9 2036249.2 2461809.7 2858682.0 3208385.2 3327760.0 3254138.2 3282592.2 3140544.2 3477457.0 3723371.2 3919188.7 4186825.2)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 16;with linespoints pt 4 ps 1.5 lc 'black'"
                  #(441957.1 808306.44 1155976.0 1565291.9 2036303.5 2555728.7 2781546.7 3398330.5 3297566.5 3192273.0 3317727.5 3270666.0 3550850.0 3708951.2 4049067.5 4347025.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 64;with linespoints pt 8 ps 2 lc 'black'"
                  #(429631.3 812819.75 1160635.0 1639762.0 2030414.5 2513414.0 2856812.7 3297184.5 3331014.0 3269242.7 3280905.7 3263276.0 3484998.5 3642425.0 4092827.5 4284347.5)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)) ))
 (plotmetrics:make-raw-plot-data
  :title "TT throughput, chaining: yes, timers: nodes"
  :x-label *parallelism-x-label*
  :y-label *scalability-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1.5 lc 'black'"
                  #(384391.62 773223.8 1206565.1 1730156.9 2113200.0 2358992.0 1758717.7 2227448.0 2548851.7 2863404.0 2995161.5)
                  #(1 2 3 4 5 6 7 8 9 10 11)
                  ";b = 1;with linespoints pt 2 ps 1.5 lc 'black'"
                  #(354489.75 836480.6 1126171.4 1635390.7 2035371.5 1992930.6 2147154.0 2138423.5 2526718.5 2786406.7 2948762.7)
                  #(1 2 3 4 5 6 7 8 9 10 11)
                  ";b = 16;with linespoints pt 4 ps 1.5 lc 'black'"
                  #(376563.28 772483.5 1209560.1 1674957.5 2084429.9 2057443.1 2041935.2 2063664.9 2541446.2 2861005.2 2945208.0)
                  #(1 2 3 4 5 6 7 8 9 10 11)
                  ";b = 64;with linespoints pt 8 ps 2 lc 'black'"
                  #(441994.53 774072.0 1207284.5 1679134.1 2000714.1 2412125.5 2034995.5 2131815.7 2332254.2 2681720.7 2958753.5)
                  #(1 2 3 4 5 6 7 8 9 10 11)) )))

(plotmetrics:boxplot-subplots
 (plotmetrics:make-parameters :plotdir *tt-directory*
                              :metric "latency"
                              :chaining-p nil
                              :percentiles '(5 25 50 75 95)
                              :timer-nodes-p nil)
 (plotmetrics:make-parameters :plotdir *tt-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 0
                              :percentiles '(5 25 50 75 95)
                              :timer-nodes-p t)
 (plotmetrics:make-parameters :plotdir *tt-directory*
                              :metric "latency"
                              :chaining-p nil
                              :percentiles '(5 25 50 75 95)
                              :single-batch-size 64
                              :timer-nodes-p nil)
 (plotmetrics:make-parameters :plotdir *tt-directory*
                              :metric "latency"
                              :chaining-p nil
                              :percentiles '(5 25 50 75 95)
                              :single-batch-size 64
                              :timer-nodes-p t))

(plotmetrics:boxplot-subplots
 (plotmetrics:make-parameters :plotdir *tt-directory*
                              :metric "latency"
                              :chaining-p nil
                              :percentiles '(5 25 50 75 95)
                              :timer-nodes-p nil)
 (plotmetrics:make-parameters :plotdir *tt-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 0
                              :percentiles '(5 25 50 75 95)
                              :timer-nodes-p t)
 (plotmetrics:make-parameters :plotdir *tt-directory*
                              :metric "latency"
                              :chaining-p t
                              :percentiles '(5 25 50 75 95)
                              :timer-nodes-p nil)
 (plotmetrics:make-parameters :plotdir *tt-directory*
                              :metric "latency"
                              :chaining-p t
                              :percentiles '(5 25 50 75 95)
                              :timer-nodes-p t))

;; Reinforcement Learner (RL)

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "RL average throughput, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(1680922.9 2424675.2 2287248.5 1294015.7 1246241.2 1233319.2 1136952.6 1123903.9 1103793.4 1087000.9 1092548.2 1094925.5)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(1711785.2 2437057.2 2189789.2 1287252.2 1272053.2 1320893.9 1166782.9 1137782.5 1161097.4 1112200.6 1088016.2 1085280.9) #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(2678192.0 2489664.2 2324118.2 1380410.1 1348291.7 1324542.2 1225062.1 1147093.4 1229783.9 1110961.9 1121581.9 1120315.9)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(2604628.2 2502438.2 2269212.5 1455544.0 1328712.2 1405556.1 1166412.2 1161010.5 1189225.5 1152946.0 1130909.1 1082524.4)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)) ))
 (plotmetrics:make-raw-plot-data
  :title "RL average throughput scalability, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *scalability-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(1.0 1.4424666 1.3607099 0.76982456 0.741403 0.73371553 0.67638594 0.6686231 0.6566591 0.64666903 0.6499693 0.6513835)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(1.0 1.4236933 1.279243 0.7519941 0.74311495 0.771647 0.6816176 0.66467595 0.67829615 0.64973146 0.63560325 0.6340053)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(1.0 0.9296064 0.86779374 0.51542616 0.5034336 0.4945658 0.4574213 0.42830887 0.45918435 0.41481787 0.41878322 0.41831052)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1.0 0.96076596 0.87122315 0.55882984 0.51013505 0.5396379 0.44782293 0.445749 0.45658165 0.44265282 0.43419212 0.4156157)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";Ideal;with points pt 1 ps 2 lc 'black'"
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)))))

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "RL average throughput, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(1680922.9 2424675.2 2287248.5 1294015.7 1246241.2 1233319.2 1136952.6 1123903.9 1103793.4 1087000.9 1092548.2 1094925.5)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(1711785.2 2437057.2 2189789.2 1287252.2 1272053.2 1320893.9 1166782.9 1137782.5 1161097.4 1112200.6 1088016.2 1085280.9)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(2678192.0 2489664.2 2324118.2 1380410.1 1348291.7 1324542.2 1225062.1 1147093.4 1229783.9 1110961.9 1121581.9 1120315.9)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ;; ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  ;; #(2604628.2 2502438.2 2269212.5 1455544.0 1328712.2 1405556.1 1166412.2 1161010.5 1189225.5 1152946.0 1130909.1 1082524.4)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11 12)
                  )))
 (plotmetrics:make-raw-plot-data
  :title "RL average throughput, chaining: enabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(1188288.0 1511209.9 1584658.5 1756806.4 1350231.4 1300183.2 1268176.4 1290276.1 1155512.1 1112531.1 1145664.0 1400716.4 1134042.1 1036868.75 1031755.0 1073831.2)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(1131186.0 1466235.0 1604894.9 1730379.7 1398124.6 1300048.9 1261743.7 1263264.9 1161307.5 1108016.4 1096770.9 1151457.6 1034699.6 1032635.3 1030900.2 1045298.5)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(1545905.5 2074135.0 2030624.5 1874889.5 1219345.9 1261338.4 1326326.4 1369022.1 1148246.7 1136785.2 1144327.5 1440367.9 1103419.4 1075507.5 1112919.1 1123014.5)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ;; ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  ;; #(1844387.0 2194695.7 2025783.5 2041962.2 1224585.1 1250362.6 1292567.5 1340078.2 1146321.1 1142535.1 1157485.1 1528875.4 1133773.5 1127445.1 1125872.7 1113893.5)
                  ;; #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ))))


(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "RL average latency, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(39.00277 159.65744 127.083374 343.5969 658.62195 842.0481 805.83813 1411.1652 2387.279 2694.2917 3734.4072 4995.6025)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(110.555046 469.23187 793.203 1523.4987 4597.6816 4986.539 4724.734 8692.466 12518.156 14867.609 18369.324 21046.982)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(389.14835 1861.0631 4495.4463 11722.734 19424.988 25311.082 28255.578 38482.926 42280.38 53142.82 59005.69 65381.12)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1836.8798 8962.264 18876.383 40487.15 56616.805 64160.285 82961.32 96308.55 107461.51 124217.9 143552.45 165327.06)
                  #(1 2 3 4 5 6 7 8 9 10 11 12)) ))
 (plotmetrics:make-raw-plot-data
  :title "RL average latency, chaining: enabled"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(55.249187 89.07856 139.97585 175.47223 310.3563 467.36115 966.87317 872.75977 939.3625 1492.4196 1656.6486 1779.9181 4882.6387 5550.462 7159.656 8570.059)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(173.16022 537.09283 558.06177 1079.7771 2390.3845 5348.5674 7228.3843 8692.243 7026.463 13595.864 15517.162 16360.195 22790.988 26216.492 29683.078 31754.078)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(676.1358 1788.1935 4388.043 8092.083 17659.295 24512.994 28213.084 32058.951 39936.336 48262.65 54738.79 52085.28 68424.336 76848.63 80762.555 84765.59) #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(2235.3582 7227.131 16482.785 26796.926 53432.332 63320.574 72857.19 82711.586 107114.97 126443.8 131272.39 119020.72 164272.03 182191.9 202451.8 210321.0)
                  #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)))))


(plotmetrics:boxplot-subplots
 (plotmetrics:make-parameters :plotdir *rl-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 0
                              :percentiles '(0 5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *rl-directory*
                              :metric "latency"
                              :chaining-p t
                              :single-batch-size 0
                              :percentiles '(0 5 25 50 75 95 100)))

(plotmetrics:boxplot-subplots
 (plotmetrics:make-parameters :plotdir *rl-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 0
                              :percentiles '(0 5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *rl-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 4
                              :percentiles '(0 5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *rl-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 16
                              :percentiles '(0 5 25 50 75 95 100))
 (plotmetrics:make-parameters :plotdir *rl-directory*
                              :metric "latency"
                              :chaining-p nil
                              :single-batch-size 64
                              :percentiles '(0 5 25 50 75 95 100)))

;; Log Processing (LP)

(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "LP average throughput, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *throughput-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(881489.0 1467437.6 1754668.0 1675910.5 1414462.7 1444632.4 1399327.7)
                  #(1 2 3 4 5 6 7)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(861697.25 1428948.5 1625790.1 1594322.2 1352193.0 1404257.7 1317106.5)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(1298597.0 1920784.7 2089389.9 2261772.0 1847892.4 1903748.7 1922837.7)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1474457.5 2150432.0 2361002.2 2273567.0 1912794.7 1874714.6 1911306.0)
                  #(1 2 3 4 5 6 7)) ))
 (plotmetrics:make-raw-plot-data
  :title "LP average throughput scalability, chaining: disabled"
  :x-label *parallelism-x-label*
  :y-label *scalability-y-label*
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(1.0 1.6647259 1.9905727 1.9012268 1.6046289 1.6388547 1.5874591)
                  #(1 2 3 4 5 6 7)
                  ";b = 1;with linespoints pt 2 ps 1 lc 'black'"
                  #(1.0 1.6582953 1.8867301 1.8502116 1.5692204 1.6296417 1.5285027)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(1.0 1.479123 1.6089594 1.7417043 1.4229915 1.4660043 1.480704)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(1.0 1.4584564 1.6012683 1.5419685 1.2972871 1.2714605 1.2962774)
                  #(1 2 3 4 5 6 7)
                  ";Ideal;with points pt 1 ps 2 lc 'black'"
                  #(1 2 3 4 5 6 7)
                  #(1 2 3 4 5 6 7)) )))


(plotmetrics:plot-subplots-from-raw-data
 (plotmetrics:make-raw-plot-data
  :title "LP average volume latency"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(4.677172 7.0630784 97.86425 4.225634 7.927032 6.881916 2.6690326)
                  #(1 2 3 4 5 6 7)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(14.40471 18.66801 25.014027 28.434868 20.502645 32.884815 27.811674)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(29.4317 25.768896 41.897713 45.428524 22.832087 28.551472 73.22001)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(41.22706 57.756573 64.549126 36.2425 28.365404 15.647511 46.208603)
                  #(1 2 3 4 5 6 7))))
 (plotmetrics:make-raw-plot-data
  :title "LP average status latency"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(4.678169 7.0638685 4.127279 141.56607 6.026956 227.4641 252.75078)
                  #(1 2 3 4 5 6 7)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(14.405634 18.669413 23.248749 29.35169 18.990517 26.730139 39.19631)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(29.434164 25.771303 40.083637 45.3974 22.13223 27.72871 84.54983)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(41.234406 57.765373 62.053368 36.273705 27.65468 15.2969675 51.153275)
                  #(1 2 3 4 5 6 7)) ))
 (plotmetrics:make-raw-plot-data
  :title "LP average geo latency"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(4.6825323 5.5485573 130.48085 2.471837 953.679 3.8071759 110.43415)
                  #(1 2 3 4 5 6 7)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(14.421447 17.233736 51.77615 25.651995 89.716515 22.772852 1702.8081)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(29.58725 24.830166 40.945206 39.454468 21.724024 26.297033 82.22926)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(41.869576 54.45613 61.822975 33.32072 27.02274 14.937238 49.99212)
                  #(1 2 3 4 5 6 7))))
 (plotmetrics:make-raw-plot-data
  :title "LP average overall latency"
  :x-label *parallelism-x-label*
  :y-label "Latency (ms)"
  :triples (nreverse
            (list ";b = 0;with linespoints pt 7 ps 1 lc 'black'"
                  #(4.6792917 6.5585017 77.49079 49.421177 322.5443 79.38439 121.951324)
                  #(1 2 3 4 5 6 7)
                  ";b = 4;with linespoints pt 2 ps 1 lc 'black'"
                  #(14.410597 18.190386 33.346306 27.812849 43.069893 27.462603 589.93866)
                  #(1 2 3 4 5 6 7)
                  ";b = 16;with linespoints pt 4 ps 1 lc 'black'"
                  #(29.484371 25.456787 40.975517 43.426796 22.229446 27.525738 79.999695)
                  #(1 2 3 4 5 6 7)
                  ";b = 64;with linespoints pt 8 ps 1.5 lc 'black'"
                  #(41.44368 56.659355 62.808487 35.278976 27.680943 15.293906 49.118)
                  #(1 2 3 4 5 6 7)) )))

(plotmetrics:boxplot-subplots
 (plotmetrics:make-parameters :plotdir *lp-directory*
                              :metric "volume latency"
                              :sampling-rate 0
                              :chaining-p nil
                              :single-batch-size 0
                              :percentiles '(25 50 75 95))
 (plotmetrics:make-parameters :plotdir *lp-directory*
                              :metric "volume latency"
                              :sampling-rate 0
                              :chaining-p nil
                              :single-batch-size 4
                              :percentiles '(25 50 75 95))
 (plotmetrics:make-parameters :plotdir *lp-directory*
                              :metric "volume latency"
                              :sampling-rate 0
                              :chaining-p nil
                              :single-batch-size 16
                              :percentiles '(25 50 75 95))
 (plotmetrics:make-parameters :plotdir *lp-directory*
                              :metric "volume latency"
                              :sampling-rate 0
                              :chaining-p nil
                              :single-batch-size 64
                              :percentiles '(25 50 75 95)))
