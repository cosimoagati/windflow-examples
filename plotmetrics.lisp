;;;; Copyright (C) 2022 Cosimo Agati

;;;; This program is free software: you can redistribute it and/or
;;;; modify it under the terms of the GNU Affero General Public License
;;;; as published by the Free Software Foundation, either version 3 of
;;;; the License, or (at your option) any later version.

;;;; You should have received a copy of the GNU AGPLv3 with this software,
;;;; if not, please visit <https://www.gnu.org/licenses/>

(eval-when (:compile-toplevel)
  (ql:quickload '(:vgplot :yason :alexandria :cl-ppcre :py4cl)))

(defpackage plotmetrics
  (:use :common-lisp)
  (:export :plot :make-parameters))

(in-package plotmetrics)

(defvar *debug* t)

(defclass plot-parameters ()
  ((batch-sizes :accessor batch-sizes  :initarg :batch-sizes
                :initform (list 0 1 2 4 8 16 32 64 128))
   (single-batch-size :accessor single-batch-size :initarg :single-batch-size
                      :initform 0)
   (parallelism-degrees :accessor pardegs :initarg :pardegs
                        :initform (list 1 5 10))
   (single-parallelism-degree :accessor single-pardeg :initarg :single-pardeg
                              :initform 1)
   (sampling-rate :accessor sampling-rate :initarg :sampling-rate
                  :initform 100)
   (tuple-generation-rate :accessor tuple-rate :initarg :tuple-rate
                          :initform 0)
   (chaining-p :accessor chaining-p :initarg :chaining-p
               :initform nil)
   (execution-mode :accessor execmode :initarg :execmode
                   :initform "default")
   (plot-by :accessor plot-by :initarg :plot-by :initform :parallelism)
   (metric-to-plot :accessor metric :initarg :metric
                   :initform "throughput")
   (percentile :accessor percentile :initarg :percentile
               :initform "mean")
   (percentiles :accessor percentiles :initarg :percentiles
                :initform (list 0 5 25 50 75 95 100))
   (compare-by :accessor compare-by :initarg :compare-by
               :initform :batch-size)
   (timer-nodes-p :accessor timer-nodes-p :initarg :timer-nodes-p
                  :initform t)
   (single-frequency :accessor single-freq :initarg :single-freq
                     :initform 2)
   (directory-to-plot :accessor plotdir :initarg :plotdir
                      :initform "")))

(defun make-parameters (&rest args)
  (apply #'make-instance 'plot-parameters args))

(defvar *default-plot-parameters* (make-parameters))

(defun set-plot-by (metric)
  (unless (member metric '(:parallelism :batch-size :chaining))
    (error "Metric must be one of :PARALLELISM, :BATCH-SIZE or :CHAINING"))
  (setf (plot-by *default-plot-parameters*) metric))

(defun set-compare-by (metric)
  (unless (member metric '(:parallelism :batch-size :chaining :execmode))
    (error "Metric must be one of :PARALLELISM, :BATCH-SIZE or :CHAINING"))
  (setf (compare-by *default-plot-parameters*) metric))

(defun set-batch-sizes (batch-sizes)
  (when (some (lambda (b) (not (member b '(0 1 2 4 8 16 32 64 128))))
              batch-sizes)
    (error "Batch size must be either one of 0 1 2 4 8 16 32 64 128"))
  (setf (batch-sizes *default-plot-parameters*) batch-sizes))

(defun set-parallelism-degrees (parallelism-degrees)
  (setf (pardegs *default-plot-parameters*) parallelism-degrees))

(defun set-single-parallelism-degree (parallelism-degree)
  (setf (single-pardeg *default-plot-parameters*) parallelism-degree))

(defun set-sampling-rate (rate)
  (unless (and (integerp rate) (not (minusp rate)))
    (error "Sampling rate must be a non-negative integer"))
  (setf (sampling-rate *default-plot-parameters*) rate))

(defun set-tuple-generation-rate (rate)
  (unless (and (integerp rate) (not (minusp rate)))
    (error "Tuple generation rate must be a non-negative integer"))
  (setf (tuple-rate *default-plot-parameters*) rate))

(defun set-chaining (chaining)
  (setf (chaining-p *default-plot-parameters*) chaining))

(defun set-metric-to-plot (metric)
  (unless (member metric '(:throughput :service-time :latency
                           :total-latency :geo-latency
                           :volume-latency :status-latency))
    (error "Invalid metric"))
  (setf (metric *default-plot-parameters*)
        (nstring-downcase (symbol-name metric))))

(defun set-percentile (percentile)
  (setf (percentile *default-plot-parameters*) percentile))

(defun set-directory-to-plot (directory)
  (unless (or (pathnamep directory) (stringp directory))
    (error "DIRECTORY must be a pathname or a string designating a path"))
  (setf (plotdir *default-plot-parameters*) directory))

(let ((unit-abbrevs '(("microseconds" . "us")
                      ("microsecond" . "us")
                      ("us" . "us")
                      ("nanoseconds" . "ns")
                      ("nanosecond" . "ns")
                      ("ns" . "ns")
                      ("milliseconds" . "ms")
                      ("millisecond" . "ms")
                      ("ms" . "ms")
                      ("seconds" . "s")
                      ("second" . "s")
                      ("s" . "s"))))
  (defun unit-to-abbrev (unit)
    "Return the time unit abbreviation for UNIT."
    (cdr (assoc unit unit-abbrevs :test #'equal))))

(let ((scale-factors '(("ns" . 1000000000)
                       ("us" . 1000000)
                       ("ms" . 1000)
                       ("s" . 1))))
  (defun time-unit-scale-factor (time-unit)
    "Return how many of the specified TIME-UNIT are required to form a second."
    (let ((key (if (symbolp time-unit)
                   (string-downcase (symbol-name time-unit))
                   time-unit)))
      (cdr (assoc key scale-factors :test #'equal)))))

(defun base-dirname (directory)
  (second (pathname-directory directory)))

(defun title-from-directory (directory)
  (let* ((words (cl-ppcre:split "-" (base-dirname directory)))
         (acronym (first words))
         (title (concatenate 'string (second words) " " (third words))))
    (concatenate 'string (string-capitalize title) " ("
                 (string-upcase acronym) ")")))

(defun ends-with (word suffix)
  "Return non-NIL if WORD ends with SUFFIX, NIL otherwise."
  (declare (type string word suffix))
  (alexandria:ends-with-subseq suffix word :test #'equal))

(defun get-json-objs-from-directory (directory)
  "Return a list containing containing pathnames to JSON files in DIRECTORY."
  (let ((file-list (directory directory)))
    (mapcar #'yason:parse
            (remove-if-not (lambda (f) (ends-with (namestring f) ".json"))
                           file-list))))

(Defun json-name-match (entry name)
  "Return non-NIL if ENTRY ends with NAME (with some transformations.
Otherwise, return NIL."
  (or (ends-with entry name)
      (ends-with (substitute #\Space #\- entry)
                 name)
      (ends-with (substitute #\Space #\- entry)
                 name)))

(defun filter-jsons-by-name (jsons name)
  "Return a list containing only the json files whose \"name\" field is NAME"
  (remove-if-not (lambda (j) (json-name-match (gethash "name" j)
                                              name))
                 jsons))

(defun filter-jsons-by-parallelism (jsons parallelism)
  "Filter out entries in JSONS not matching PARALLELISM.
Return a brand new list, the original list is left untouched."
  (remove-if-not (lambda (j) (= (first (gethash "parallelism" j))
                                parallelism))
                 jsons))

(defun filter-jsons-by-batch-size (jsons batch-size)
  "Filter out entries in JSONS not matching BATCH-SIZE.
Return a brand new list, the original list is left untouched."
  (remove-if-not (lambda (j) (= (first (gethash "batch size" j))
                                batch-size))
                 jsons))

(defun filter-jsons-by-chaining (jsons chaining-p)
  "Filter out entries in JSONS not matching CHAINING-P.
Return a brand new list, the original list is left untouched."
  (remove-if-not (lambda (j) (eql (gethash "chaining enabled" j)
                                  chaining-p))
                 jsons))

(defun get-sampling-rate (json)
  "Extract tuple sampling rate from the specified JSON object."
  (multiple-value-bind (val1 present1-p) (gethash "sampling_rate" json)
    (multiple-value-bind (val2 present2-p) (gethash "sampling rate" json)
      (if present1-p val1 (values val2 present2-p)))))

(defun filter-jsons-by-sampling-rate (jsons sampling-rate)
  "Filter out entries in JSONS not matching SAMPLING-RATE.
Return a brand new list, the original list is left untouched."
  (remove-if-not (lambda (j) (= (get-sampling-rate j)
                                sampling-rate))
                 jsons))

(defun contains-frequency-fields-p (json)
  (let ((keys (alexandria:hash-table-keys json)))
    (some (lambda (k) (ends-with k "frequency")) keys)))

(defun frequency-fields-equal-value-p (json value)
  (let ((frequency-keys (remove-if-not (lambda (k)
                                         (ends-with k "frequency"))
                                       (alexandria:hash-table-keys json))))
    (every (lambda (v) (equal v value))
           (mapcar (lambda (k) (gethash k json)) frequency-keys))))

(defun filter-jsons-by-frequency (jsons frequency)
  (remove-if-not (lambda (j) (frequency-fields-equal-value-p j frequency))
                 jsons))

(defun contains-timer-node-key (json)
  (let ((keys (alexandria:hash-table-keys json)))
    (member "using timer nodes" keys :test #'equal)))

(defun filter-jsons-by-timernode-impl (jsons timer-nodes-p)
  (remove-if-not (lambda (j)
                   (eql (gethash "using timer nodes" j) timer-nodes-p))
                 jsons))

(defun filter-jsons (parameters jsons)
  (with-accessors ((plot-by plot-by) (compare-by compare-by))
      parameters
    (setf jsons (filter-jsons-by-name jsons (metric parameters))
          jsons (filter-jsons-by-sampling-rate jsons (sampling-rate parameters))
          jsons (filter-jsons-by-tuple-rate jsons (tuple-rate parameters)))
    (unless (or (eql plot-by :parallelism) (eql compare-by :parallelism))
      (setf jsons (filter-jsons-by-parallelism jsons
                                               (single-pardeg parameters))))
    (unless (or (eql plot-by :batch-size) (eql compare-by :batch-size))
      (setf jsons (filter-jsons-by-batch-size jsons
                                              (single-batch-size parameters))))
    (unless (or (eql plot-by :chaining) (eql compare-by :chaining))
      (setf jsons (filter-jsons-by-chaining jsons (chaining-p parameters))))
    (when (and (some #'contains-frequency-fields-p jsons)
               (not (eql compare-by :frequency)))
      (setf jsons (filter-jsons-by-frequency jsons (single-freq parameters))))
    (unless (eql compare-by :execmode)
      (setf jsons (filter-jsons-by-execmode jsons (execmode parameters))))
    (when (and (some #'contains-timer-node-key jsons)
               (not (eql compare-by :timernode-impl)))
      (setf jsons (filter-jsons-by-timernode-impl jsons
                                                  (timer-nodes-p parameters))))
    (setf jsons (ecase plot-by
                  (:parallelism (sort-jsons-by-parallelism jsons))
                  (:batch-size (sort-jsons-by-batch-size jsons))
                  (:chaining (sort-jsons-by-chaining jsons))))
    jsons))

(defun get-tuple-rate (json)
  "Extract tuple generation rate from the specified JSON object."
  (multiple-value-bind (val1 present1-p) (gethash "tuple_rate" json)
    (multiple-value-bind (val2 present2-p) (gethash "tuple rate" json)
      (if present1-p val1 (values val2 present2-p)))))

(defun filter-jsons-by-tuple-rate (jsons tuple-rate)
  "Filter out entries in JSONS not matching TUPLE-RATE.
Return a brand new list, the original list is left untouched."
  (remove-if-not (lambda (j) (= (get-tuple-rate j)
                                tuple-rate))
                 jsons))

(defun filter-jsons-by-execmode (jsons execmode)
  "Filter out entries in JSONS not matching EXECMODE.
Return a brand new list, the original list is left untouched."
  (remove-if-not (lambda (j) (string= (gethash "execution mode" j)
                                      execmode))
                 jsons))

(defun percentile-to-dictkey (kind)
  "From KIND, return the right string representing the key to index JSONs."
  (cond ((member kind '("0th" "5th" "25th" "50th" "75th" "95th" "100th")
                 :test #'equal)
         (concatenate 'string kind " percentile"))
        ((member kind '("0" "5" "25" "50" "75" "95" "100")
                 :test #'equal)
         (concatenate 'string kind "th percentile"))
        ((member kind '(0 5 25 50 75 95 100))
         (concatenate 'string (write-to-string kind) "th percentile"))
        (t kind)))

(defun get-x-label (plot-by)
  (ecase plot-by
    (:parallelism "Parallelism degree for each node")
    (:batch-size "Initial batch size")
    (:chaining "Chaining enabled?")))

(defun get-y-label (name time-unit)
  "Return the proper Y label name from NAME and TIME-UNIT."
  (let* ((time-unit-string (let ((abbrev (unit-to-abbrev time-unit)))
                             (if abbrev abbrev "unknown unit")))
         (unit-string (if (not (search "throughput" (string-downcase name)))
                          time-unit-string
                          "tuples per second")))
    (concatenate 'string (string-capitalize (substitute #\Space #\- name))
                 " (" unit-string ")")))

(defun get-y-axis (name jsons percentile time-unit)
  (let ((map-func (if (not (search name "throughput"))
                      (lambda (j)
                        (gethash (percentile-to-dictkey percentile) j))
                      (lambda (j)
                        (* (time-unit-scale-factor (unit-to-abbrev time-unit))
                           (gethash (percentile-to-dictkey percentile) j))))))
    (mapcar map-func jsons)))

(defun scale-by-base-value (base-value y measure)
  (if (search "throughput" measure)
      (/ y base-value)
      (/ base-value y)))

(defun get-scaled-y-axis (name jsons percentile base-value)
  (let ((unscaled-y-axis (mapcar (lambda (j) (gethash percentile j))
                                 jsons)))
    (mapcar (lambda (y) (scale-by-base-value base-value y name))
            unscaled-y-axis)))

(defun get-efficiency-y-axis (name jsons percentile base-value)
  (let ((scaled-y-axis (get-scaled-y-axis name jsons
                                          percentile base-value)))
    (dotimes (i (length scaled-y-axis))
      (setf (elt scaled-y-axis i) (/ (elt scaled-y-axis i)
                                     (1+ i))))
    scaled-y-axis))

(defun get-percentile-values (percentile-map percentile-keys)
  (mapcar (lambda (p) (let ((key (concatenate 'string (write-to-string p)
                                              "th percentile")))
                        (gethash key percentile-map)))
          percentile-keys))

(defun get-x-axis (plot-by jsons)
  (ecase plot-by
    (:parallelism (mapcar (lambda (j) (first (gethash "parallelism" j)))
                          jsons))
    (:batch-size (mapcar (lambda (j) (first (gethash "batch size" j)))
                         jsons))
    (:chaining (mapcar (lambda (x) (if x 1 0))
                       (mapcar (lambda (j) (gethash "chaining" j))
                               jsons)))))

(defun chaining-to-string (chaining-p)
  (if chaining-p "enabled" "disabled"))

(defun tuple-rate-to-string (rate)
  (if (plusp rate) (write-to-string rate) "unlimited"))

(defun title-for-plot (parameters)
  (let ((initial-title (concatenate
                        'string (title-from-directory (plotdir parameters))
                        " - " (substitute #\Space #\-
                                          (string-capitalize
                                           (metric parameters)))
                        " (" (percentile parameters) ") ")))
    (symbol-macrolet ((chaining (chaining-to-string (chaining-p parameters)))
                      (pardeg (write-to-string (single-pardeg parameters)))
                      (batch-size (write-to-string
                                   (single-batch-size parameters)))
                      (tuple-rate (tuple-rate-to-string
                                   (tuple-rate parameters))))
      (ecase (plot-by parameters)
        (:parallelism (ecase (compare-by parameters)
                        (:batch-size
                         (concatenate 'string initial-title
                                      "(chaining: " chaining
                                      ") (generation rate: " tuple-rate ")"))
                        (:chaining
                         (concatenate 'string initial-title
                                      "(batch size: " batch-size
                                      ") (generation rate: " tuple-rate ")"))
                        (:execmode
                         (concatenate 'string initial-title
                                      "(chaining " chaining
                                      ") (generation rate: " tuple-rate
                                      ") (batch size: " batch-size ")"))
                        (:frequency
                         (concatenate 'string initial-title
                                      "(chaining: " chaining
                                      ") (generation rate: "  tuple-rate
                                      ") (batch-size: " batch-size))))
        (:batch-size (ecase (compare-by parameters)
                       (:parallelism
                        (concatenate 'string initial-title
                                     "(chaining " chaining
                                     ") (generation rate: " tuple-rate ")"))
                       (:chaining
                        (concatenate 'string initial-title
                                     " (parallelism degree per node: " pardeg
                                     ") (generation rate: " tuple-rate ")"))
                       (:execmode
                        (concatenate 'string initial-title
                                     " (parallelism degree per node: " pardeg
                                     ") (chaining: " chaining
                                     ") (generation rate: " tuple-rate ")"))))
        (:chaining (error "Not implemented yet"))))))

(defun sort-jsons-by-parallelism (jsons)
  (sort jsons #'< :key (lambda (j) (first (gethash "parallelism" j)))))

(defun sort-jsons-by-batch-size (jsons)
  (sort jsons #'< :key (lambda (j) (first (gethash "batch size" j)))))

(defun sort-jsons-by-chaining (jsons)
  (sort jsons (lambda (a b) (and a (not b)))
        :key (lambda (j) (gethash "chaining" j))))

(defun plot-from-triples (triples)
  (apply #'vgplot:plot triples))

(defun save-plot (name)
  (vgplot:print-plot (pathname name)
                     :terminal "png size 1280,960"))

(defun draw-boxplot (&key title x-label y-label y-axis x-axis)
  (py4cl:import-module "matplotlib.pyplot" :as "plt")
  (plt:figure)
  (plt:xlabel x-label)
  (plt:ylabel y-label)
  (plt:title title :loc "right" :y 1.00)
  (plt:grid t)
  (plt:boxplot y-axis :positions x-axis)
  (plt:show)
  (plt:close "all"))

(defun boxplot-title (parameters)
  (with-accessors ((plotdir plotdir) (metric metric)
                   (batch-size single-batch-size)
                   (chaining-p chaining-p) (percentiles percentiles))
      parameters
    (concatenate 'string (title-from-directory plotdir) " - "
                 (substitute #\Space #\- (string-capitalize metric))
                 " (batch size: " (write-to-string batch-size)
                 ") (chaining: " (if chaining-p "enabled " "disabled")
                 ") (percentiles: " (write-to-string percentiles) ")")))

(defun boxplot (&optional (parameters *default-plot-parameters*) jsons
                  image-path)
  (unless jsons
    (setf jsons (get-json-objs-from-directory (plotdir parameters))))
  (setf jsons (filter-jsons parameters jsons))
  (unless jsons
    (format t "No data found with the specified parameters, not plotting...")
    (return-from boxplot))
  (let ((time-unit (gethash "time unit" (first jsons))))
    (let ((title (boxplot-title parameters))
          (xlabel (get-x-label (plot-by parameters)))
          (ylabel (get-y-label (metric parameters) time-unit))
          (x-axis (get-x-axis (plot-by parameters) jsons))
          (y-axis (mapcar (lambda (j)
                            (get-percentile-values j (percentiles parameters)))
                          jsons)))
      (draw-boxplot :title title :x-label xlabel :y-label ylabel
                    :x-axis x-axis :y-axis y-axis))))

(defmacro push-to-triples (x-axis y-axis label triples)
  `(progn
     (push ,label ,triples)
     (push ,y-axis ,triples)
     (push ,x-axis ,triples)))

(defun get-triples-for-parallelism-comparison (parameters jsons)
  (with-accessors ((name metric) (pardegs pardegs)
                   (percentile percentile))
      parameters
    (let ((time-unit (gethash "time unit" (first jsons)))
          plot-triples)
      (dolist (pardeg pardegs plot-triples)
        (let ((current-jsons (filter-jsons-by-parallelism jsons pardeg)))
          (when current-jsons
            (let ((x-axis (get-x-axis (plot-by parameters) current-jsons))
                  (y-axis (get-y-axis name current-jsons percentile time-unit))
                  (label
                    (concatenate 'string "Parallelism degree: "
                                 (write-to-string
                                  (gethash "parallelism"
                                           (first current-jsons))))))
              (push-to-triples x-axis y-axis label plot-triples))))))))

(defun get-triples-for-batch-size-comparison (parameters jsons)
  "Get the values to plot for a batch size comparison.
Data is extracted from JSONS, whose entries must already all contain the
same tuple generation rate, sampling rate and chaining value.
BATCH-SIZES is a list containing the batch sizes to be compared.
PERCENTILE and TIME-UNIT are used to extract the appropriate Y values."
  (with-accessors ((name metric) (batch-sizes batch-sizes)
                   (percentile percentile))
      parameters
    (let ((time-unit (gethash "time unit" (first jsons)))
          plot-triples)
      (dolist (batch-size batch-sizes plot-triples)
        (let ((current-jsons (filter-jsons-by-batch-size jsons
                                                         batch-size)))
          (when current-jsons
            (let ((x-axis (get-x-axis (plot-by parameters) current-jsons))
                  (y-axis (get-y-axis name current-jsons percentile
                                      time-unit))
                  (label
                    (concatenate 'string "Batch size: "
                                 (write-to-string
                                  (gethash "batch size"
                                           (first current-jsons))))))
              (push-to-triples x-axis y-axis label plot-triples))))))))

(defun get-triples-for-chaining-comparison (parameters jsons)
  (with-accessors ((name metric) (percentile percentile)) parameters
    (let ((time-unit (gethash "time unit" (first jsons)))
          plot-triples)
      (dolist (chaining-p '(nil t) plot-triples)
        (let ((current-jsons (filter-jsons-by-chaining jsons chaining-p)))
          (when current-jsons
            (let ((x-axis (get-x-axis (plot-by parameters) current-jsons))
                  (y-axis (get-y-axis name current-jsons percentile
                                      time-unit))
                  (label (concatenate 'string "Chaining: "
                                      (chaining-to-string chaining-p))))
              (push-to-triples x-axis y-axis label plot-triples))))))))

(defun get-triples-for-execmode-comparison (parameters jsons)
  (with-accessors ((name metric) (percentile percentile)) parameters
    (let ((time-unit (gethash "time unit" (first jsons)))
          plot-triples)
      (dolist (execmode '("deterministic" "default") plot-triples)
        (let ((current-jsons (filter-jsons-by-execmode jsons execmode)))
          (when current-jsons
            (let ((x-axis (get-x-axis (plot-by parameters) current-jsons))
                  (y-axis (get-y-axis name current-jsons percentile
                                      time-unit))
                  (label (concatenate 'string "Execution mode: " execmode)))
              (push-to-triples x-axis y-axis label plot-triples))))))))

(defun get-triples-for-frequency-comparison (parameters jsons)
  (with-accessors ((name metric) (percentile percentile)) parameters
    (let ((time-unit (gethash "time unit" (first jsons)))
          plot-triples)
      (dolist (frequency '(2 4 6 8 10) plot-triples)
        (let ((current-jsons (filter-jsons-by-frequency jsons frequency)))
          (when current-jsons
            (let ((x-axis (get-x-axis (plot-by parameters) current-jsons))
                  (y-axis (get-y-axis name current-jsons percentile
                                      time-unit))
                  (label (concatenate 'string
                                      "Output frequency for all operators:"
                                      (write-to-string frequency))))
              (push-to-triples x-axis y-axis label plot-triples))))))))

(defun get-triples (parameters jsons)
  (ecase (compare-by parameters)
    (:parallelism (get-triples-for-parallelism-comparison parameters jsons))
    (:batch-size (get-triples-for-batch-size-comparison parameters jsons))
    (:chaining (get-triples-for-chaining-comparison parameters jsons))
    (:execmode (get-triples-for-execmode-comparison parameters jsons))
    (:frequency (get-triples-for-frequency-comparison parameters jsons))))

(defun plot (&optional (parameters *default-plot-parameters*) jsons
               image-path)
  (unless jsons
    (setf jsons (get-json-objs-from-directory (plotdir parameters))))
  (setf jsons (filter-jsons parameters jsons))
  (unless jsons
    (format t "No data found with the specified parameters, not plotting...")
    (return-from plot))
  (let ((time-unit (gethash "time unit" (first jsons))))
    (let ((title (title-for-plot parameters))
          (xlabel (get-x-label (plot-by parameters)))
          (ylabel (get-y-label (metric parameters) time-unit)))
      (vgplot:title title)
      (vgplot:xlabel xlabel)
      (vgplot:ylabel ylabel)
      (vgplot:grid t)
      (let ((plot-triples (get-triples parameters jsons)))
        (when *debug*
          (print plot-triples))
        (plot-from-triples plot-triples))
      (when image-path
        (save-plot image-path)))))
