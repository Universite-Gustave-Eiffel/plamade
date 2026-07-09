INSERT INTO cbs_uge_input.nm_conf (confid, confreflorder, confmaxsrcdist, confmaxrefldist, confdistbuildingsreceivers,
                                   confthreadnumber, confdiffvertical, confdiffhorizontal, confskiplday,
                                   confskiplevening, confskiplnight, confskiplden, confexportsourceid, wall_alpha)
VALUES (1, 0, 250, 50, 5, 1, false, false, true, true, false, false, true, 0.1),
       (2, 0, 250, 50, 5, 1, false, false, true, true, false, false, false, 0.1),
       (3, 1, 800, 250, 5, 3, false, true, true, true, false, false, true, 0.1),
       (4, 1, 800, 250, 5, 16, false, true, true, true, false, false, true, 0.1),
       (5, 1, 800, 800, 5, 16, false, true, true, true, false, false, true, 0.1),
       (6, 3, 800, 800, 5, 16, false, true, true, true, false, false, true, 0.1);



