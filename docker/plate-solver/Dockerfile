# taken from https://github.com/panoptes/plate-solver/blob/91aa42411a83fc7a1986624c30e780c5610bc588/Dockerfile

FROM debian:buster-slim

ADD http://data.astrometry.net/4200/index-4205-00.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-01.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-02.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-03.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-04.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-05.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-06.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-07.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-08.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-09.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-10.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4205-11.fits /usr/share/astrometry/

ADD http://data.astrometry.net/4200/index-4206-00.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-01.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-02.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-03.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-04.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-05.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-06.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-07.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-08.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-09.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-10.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4206-11.fits /usr/share/astrometry/

ADD http://data.astrometry.net/4200/index-4207-00.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-01.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-02.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-03.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-04.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-05.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-06.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-07.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-08.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-09.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-10.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4207-11.fits /usr/share/astrometry/

ADD http://data.astrometry.net/4200/index-4208.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4209.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4210.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4211.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4212.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4213.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4214.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4215.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4216.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4217.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4218.fits /usr/share/astrometry/
ADD http://data.astrometry.net/4200/index-4219.fits /usr/share/astrometry/

COPY ./bin/ /app/
RUN apt-get update && \
    apt-get install --no-install-recommends -y wget astrometry.net && \
    useradd -ms /bin/bash solve-user && \
    chown -R solve-user:solve-user /usr/share/astrometry && \
    apt-get autoremove --purge -y && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/*

USER solve-user
WORKDIR /tmp
CMD ["/usr/bin/solve-field"]
